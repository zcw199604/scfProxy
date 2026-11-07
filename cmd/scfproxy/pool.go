package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/zcw199604/scfproxy/internal/deploy"
	"github.com/zcw199604/scfproxy/pkg/scfpool"
)

func poolCommand(ctx context.Context, args []string, in io.Reader, out io.Writer, interactive bool) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprintln(out, "用法: scfproxy pool <init|status|switch|unban|replenish|rotate|destroy> [参数]")
		return nil
	}
	command := args[0]
	switch command {
	case "init", "status", "switch", "unban", "replenish", "rotate", "destroy":
	default:
		return fmt.Errorf("未知pool子命令 %q", command)
	}
	fs := flags("pool "+command, out)
	var opts scfpool.Options
	fs.StringVar(&opts.PoolFile, "pool", "pool.json", "固定IP池文件（不含密钥）")
	creating := command == "init" || command == "replenish" || command == "rotate"
	modifying := creating || command == "destroy"
	var yes bool
	if modifying {
		fs.BoolVar(&yes, "yes", false, "确认创建/删除真实云资源")
		fs.StringVar(&opts.Prefix, "prefix", "scfproxy", "仅管理此前缀且Description=scfproxy的函数")
	}
	var regions, packagePath string
	if creating {
		fs.StringVar(&regions, "regions", "", "按优先级排列的候选地域，逗号分隔，至少size个不同地域")
		fs.IntVar(&opts.Size, "size", 2, "期望active数量（含主用与备用）")
		fs.StringVar(&packagePath, "package", "dist/scf-server.zip", "函数代码包")
		fs.StringVar(&opts.Runtime, "runtime", "Go1", "创建函数运行时")
		fs.Int64Var(&opts.Timeout, "timeout", 900, "函数执行超时秒数")
		fs.Int64Var(&opts.IdleTimeout, "idle-timeout", 600, "WS空闲超时秒数")
		fs.Int64Var(&opts.Memory, "memory", 64, "函数内存MB")
		fs.IntVar(&opts.MaxConcurrency, "max-concurrency", 10, "单实例请求并发数0~100，0不设置")
		fs.IntVar(&opts.MaxAttempts, "max-attempts", 3, "单个补位的IP冲突重建次数上限")
		fs.DurationVar(&opts.EgressIPTimeout, "eip-timeout", 3*time.Minute, "每次创建等待EIP的总超时")
	}
	reason := "manual"
	if command == "switch" || command == "rotate" {
		fs.StringVar(&reason, "reason", "manual", "退役原因，写入池文件，请勿包含密钥")
		fs.DurationVar(&opts.BanTTL, "ban-ttl", 24*time.Hour, "IP封禁有效期，0使用默认24h，负数不记录封禁")
	}
	var unbanIP string
	var unbanAll bool
	if command == "unban" {
		fs.StringVar(&unbanIP, "ip", "", "解除指定IP的封禁")
		fs.BoolVar(&unbanAll, "all", false, "解除全部IP封禁")
	}
	if err := parse(fs, args[1:]); err != nil {
		return err
	}
	if command == "unban" {
		if (unbanIP == "") == !unbanAll {
			return errors.New("必须且只能指定 --ip 或 --all")
		}
		if err := scfpool.Unban(opts.PoolFile, unbanIP); err != nil {
			return err
		}
		fmt.Fprintln(out, "已解除本地IP封禁。")
		return nil
	}
	if command == "status" {
		state, err := scfpool.Read(opts.PoolFile)
		if err != nil {
			return err
		}
		fmt.Fprintln(out, "active（第1项为主用，其余为备用）:")
		for i, e := range state.Active {
			role := "备用"
			if i == 0 {
				role = "主用"
			}
			fmt.Fprintf(out, "  %s %s %s %s %s\n", role, e.Region, e.Function, e.EgressIP, e.URL)
		}
		fmt.Fprintln(out, "retired（待删除）:")
		for _, e := range state.Retired {
			fmt.Fprintf(out, "  %s %s %s reason=%q\n", e.Region, e.Function, e.EgressIP, e.Reason)
		}
		fmt.Fprintln(out, "banned_ips（未过期）:")
		for _, ban := range state.BannedIPs {
			if remaining := time.Until(ban.ExpiresAt); remaining > 0 {
				fmt.Fprintf(out, "  %s 剩余=%s reason=%q\n", ban.IP, remaining.Round(time.Second), ban.Reason)
			}
		}
		return nil
	}
	if command == "switch" {
		return switchPool(out, opts.PoolFile, reason, opts.BanTTL)
	}
	if creating {
		if opts.MaxConcurrency < 0 || opts.MaxConcurrency > 100 {
			return errors.New("max-concurrency 必须在0~100之间")
		}
		opts.DisableConcurrency = opts.MaxConcurrency == 0
		var err error
		opts.Regions, err = deploy.ResolveRegions(regions, "")
		if err != nil {
			return err
		}
		if opts.Size < 1 || len(opts.Regions) < opts.Size {
			return errors.New("size必须大于0且候选地域数不得小于size")
		}
		if opts.MaxAttempts < 1 || opts.EgressIPTimeout <= 0 {
			return errors.New("max-attempts和eip-timeout必须大于0")
		}
		opts.Token = os.Getenv("SCFPROXY_TOKEN")
		if opts.Token == "" {
			return errors.New("缺少 SCFPROXY_TOKEN，请使用gen-token生成")
		}
		opts.Package, err = loadPackage(packagePath)
		if err != nil {
			return err
		}
	}
	fmt.Fprintf(out, "pool %s 计划: 文件=%s 前缀=%s 地域=%v 目标数量=%d 单实例并发=%d；将创建或删除计费云资源。\n", command, opts.PoolFile, opts.Prefix, opts.Regions, opts.Size, opts.MaxConcurrency)
	if err := confirm(in, out, interactive, yes); err != nil {
		return err
	}
	manager, err := scfpool.NewManagerFromEnv(opts)
	if err != nil {
		return err
	}
	switch command {
	case "init":
		if err = manager.Init(ctx); err == nil {
			fmt.Fprintln(out, "池初始化完成；请运行 pool status 查看主备出口。")
		}
	case "replenish", "rotate":
		if command == "rotate" {
			if err = switchPool(out, opts.PoolFile, reason, opts.BanTTL); err != nil {
				return err
			}
		}
		report, e := manager.Replenish(ctx)
		printPoolReport(out, report)
		err = e
	case "destroy":
		err = manager.Destroy(ctx)
		if err == nil {
			fmt.Fprintln(out, "池函数已删除，active与retired清空；banned_ips保留。")
		}
	}
	return err
}
func switchPool(out io.Writer, path, reason string, ttl time.Duration) error {
	old, next, err := scfpool.Switch(path, reason, ttl)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "主用已切换: %s %s -> %s %s；旧函数待replenish删除。\n", old.Region, old.EgressIP, next.Region, next.EgressIP)
	return nil
}
func printPoolReport(out io.Writer, report scfpool.Report) {
	for _, e := range report.Deleted {
		fmt.Fprintf(out, "已删除 %s %s %s 耗时=%s\n", e.Region, e.Function, e.EgressIP, e.Duration)
	}
	for _, e := range report.Created {
		fmt.Fprintf(out, "已新建 %s %s %s 耗时=%s\n", e.Region, e.Function, e.EgressIP, e.Duration)
	}
}
