// scfproxy 提供部署管理及隧道检测工具；仅显式执行子命令才访问网络。
package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/zcw199604/scfproxy/internal/deploy"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	info, err := os.Stdin.Stat()
	interactive := err == nil && info.Mode()&os.ModeCharDevice != 0
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, interactive); err != nil {
		fmt.Fprintln(os.Stderr, deploy.SafeError(err))
		os.Exit(1)
	}
}
func flags(name string, out io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(out)
	return fs
}
func parse(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("多余参数: %s", strings.Join(fs.Args(), " "))
	}
	return nil
}
func confirm(in io.Reader, out io.Writer, interactive, yes bool) error {
	if yes {
		return nil
	}
	if !interactive {
		return errors.New("非交互环境必须指定 --yes")
	}
	fmt.Fprint(out, "输入 yes 确认执行: ")
	line, err := bufio.NewReader(in).ReadString('\n')
	if err != nil && err != io.EOF {
		return err
	}
	if strings.TrimSpace(line) != "yes" {
		return errors.New("操作已取消")
	}
	return nil
}
func run(ctx context.Context, args []string, in io.Reader, out io.Writer, interactive bool) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprintln(out, "用法: scfproxy <gen-token|deploy|list|delete|check|bench|concurrency|pool> [参数]\n使用 scfproxy <子命令> -h 查看帮助。部署前请先 make package-server。")
		return nil
	}
	var err error
	switch args[0] {
	case "gen-token":
		fs := flags("gen-token", out)
		fs.Usage = func() {
			fmt.Fprintln(out, "用法: scfproxy gen-token\n生成32字节随机token，仅输出，不写文件。")
		}
		if err = parse(fs, args[1:]); err == nil {
			var token [32]byte
			if _, err = rand.Read(token[:]); err == nil {
				fmt.Fprintln(out, hex.EncodeToString(token[:]))
			}
		}
	case "pool":
		err = poolCommand(ctx, args[1:], in, out, interactive)
	case "concurrency":
		err = concurrencyCommand(ctx, args[1:], out)
	case "deploy", "list", "delete":
		err = manage(ctx, args[0], args[1:], in, out, interactive)
	case "check", "bench":
		err = probe(ctx, args[0], args[1:], out)
	default:
		err = fmt.Errorf("未知子命令 %q，请使用 -h 查看帮助", args[0])
	}
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	return err
}
func loadPackage(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取代码包失败，请先 make package-server: %w", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		return nil, errors.New("代码包必须是有效的zip，请先 make package-server")
	}
	expected := map[string]bool{"scf-server": false, "scf_bootstrap": false}
	for _, f := range zr.File {
		seen, ok := expected[f.Name]
		if !ok || seen || f.UncompressedSize64 == 0 {
			return nil, errors.New("zip根目录必须仅包含非空的 scf-server 和 scf_bootstrap")
		}
		expected[f.Name] = true
		if f.Mode()&0111 == 0 {
			return nil, fmt.Errorf("zip中的 %s 必须可执行", f.Name)
		}
	}
	if !expected["scf-server"] || !expected["scf_bootstrap"] {
		return nil, errors.New("zip缺少 scf-server 或 scf_bootstrap")
	}
	return b, nil
}
func manage(ctx context.Context, command string, args []string, in io.Reader, out io.Writer, interactive bool) error {
	fs := flags(command, out)
	regions := fs.String("r", "", "逗号分隔的地域，例如 ap-guangzhou,ap-shanghai")
	area := fs.String("a", "", "地域组: domestic|foreign|all，与-r互斥")
	prefix := fs.String("prefix", "scfproxy", "函数名前缀")
	var o deploy.Options
	var path string
	var yes bool
	waitTimeout := 3 * time.Minute
	eipTimeout := 3 * time.Minute
	if command != "list" {
		fs.BoolVar(&yes, "yes", false, "明确确认执行，无需交互")
		fs.StringVar(&path, "o", "endpoints.txt", "合并或清理的endpoints文件")
	}
	packagePath := ""
	if command == "deploy" {
		fs.BoolVar(&o.FixedIP, "fixed-ip", false, "开启固定公网出口IP（同账号同地域共享）")
		fs.DurationVar(&eipTimeout, "eip-timeout", 3*time.Minute, "等待固定公网IP出现的总超时")
		fs.BoolVar(&o.UpdateCode, "update-code", false, "更新已有且属于scfproxy的函数代码，不修改配置或token")
		fs.BoolVar(&o.UpdateConfig, "update-config", false, "更新已有且归属匹配函数的内存和单实例并发配置")
		fs.IntVar(&o.MaxConcurrency, "max-concurrency", 10, "单实例请求并发数0~100；0创建时不设置，update-config时取消")
		fs.IntVar(&o.Count, "n", 1, "每地域函数数量")
		fs.StringVar(&packagePath, "package", "dist/scf-server.zip", "make package-server生成的zip")
		fs.StringVar(&o.Runtime, "runtime", "Go1", "运行时，实测可改CustomRuntime")
		fs.Int64Var(&o.Timeout, "timeout", 900, "执行超时秒数，必须>=idle-timeout")
		fs.Int64Var(&o.IdleTimeout, "idle-timeout", 600, "WS空闲超时秒数，范围需上云确认")
		fs.Int64Var(&o.Memory, "memory", 64, "内存MB")
		fs.IntVar(&o.Concurrency, "concurrency", 8, "最多并发创建数量")
		fs.DurationVar(&waitTimeout, "wait-timeout", 3*time.Minute, "每个函数等待Active的总超时")
	}
	if err := parse(fs, args); err != nil {
		return err
	}
	resolved, err := deploy.ResolveRegions(*regions, *area)
	if err != nil {
		return err
	}
	if err = deploy.ValidatePrefix(*prefix); err != nil {
		return err
	}
	o.Regions = resolved
	o.Prefix = *prefix
	if command == "deploy" {
		if waitTimeout <= 0 || eipTimeout <= 0 {
			return errors.New("wait-timeout和eip-timeout必须大于0")
		}
		o.Token = os.Getenv("SCFPROXY_TOKEN")
		if o.Token == "" {
			return errors.New("缺少 SCFPROXY_TOKEN，请使用 scfproxy gen-token 生成")
		}
		plan, err := deploy.Plan(o)
		if err != nil {
			return err
		}
		o.Package, err = loadPackage(packagePath)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "部署计划: runtime=%s timeout=%ds idle-timeout=%ds memory=%dMB max-concurrency=%d update-code=%v update-config=%v 共%d个函数\n", o.Runtime, o.Timeout, o.IdleTimeout, o.Memory, o.MaxConcurrency, o.UpdateCode, o.UpdateConfig, len(plan))
		if o.FixedIP {
			fmt.Fprintln(out, "固定出口IP已开启；固定IP网关可能产生额外费用。 ")
			if o.Count > 1 {
				fmt.Fprintln(out, "警告：同账号同地域内的函数共享同一EIP；-n > 1 不会获得不同出口IP。 ")
			}
		}
		for _, e := range plan {
			fmt.Fprintf(out, "  %s %s\n", e.Region, e.Function)
		}
		if err = confirm(in, out, interactive, yes); err != nil {
			return err
		}
	}
	factory, err := deploy.NewSDKFactoryFromEnv()
	if err != nil {
		return err
	}
	manager := &deploy.Manager{Factory: factory, WaitTimeout: waitTimeout, EgressIPTimeout: eipTimeout}
	if command == "deploy" {
		results, err := manager.Deploy(ctx, o)
		if err != nil {
			return err
		}
		var endpoints []deploy.Endpoint
		failed := 0
		for _, r := range results {
			if r.Updated {
				fmt.Fprintf(out, "%s %s 代码已更新，保留现有运行时和token\n", r.Region, r.Function)
			} else if r.Existed && !o.UpdateConfig {
				fmt.Fprintf(out, "%s %s 已存在，未更新代码或token；更新请先 delete 再 deploy\n", r.Region, r.Function)
			}
			if r.ConfigUpdated {
				fmt.Fprintf(out, "%s %s 配置已更新：memory=%dMB max-concurrency=%d\n", r.Region, r.Function, o.Memory, o.MaxConcurrency)
			}
			if r.Err != nil {
				failed++
				fmt.Fprintf(out, "失败 %s %s: %v\n", r.Region, r.Function, deploy.SafeError(r.Err))
			} else {
				fmt.Fprintf(out, "成功 %s %s %s 出口IP=%s\n", r.Region, r.Function, r.URL, r.EgressIP)
				endpoints = append(endpoints, r.Endpoint)
			}
		}
		if len(endpoints) > 0 {
			if err = deploy.MergeEndpoints(path, endpoints); err != nil {
				return fmt.Errorf("云端创建已有结果，但写入endpoints失败，请保存上方地址: %w", err)
			}
		}
		fmt.Fprintf(out, "汇总: 成功%d 失败%d，输出 %s\n", len(endpoints), failed, path)
		if failed > 0 {
			return errors.New("部分函数部署失败；已创建函数不会自动回滚，请 list 检查并按需 delete")
		}
		return nil
	}
	items, err := manager.List(ctx, resolved, *prefix)
	if err != nil {
		return err
	}
	for _, item := range items {
		concurrency := "-"
		if item.MaxConcurrency != nil {
			concurrency = strconv.FormatUint(*item.MaxConcurrency, 10)
		}
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\t出口IP=%s\t单实例并发=%s\n", item.Region, item.Function, item.Status, item.URL, item.EgressIP, concurrency)
		if item.URLWarning != nil {
			fmt.Fprintf(out, "  URL获取提示: %v\n", deploy.SafeError(item.URLWarning))
		}
	}
	if command == "list" {
		fmt.Fprintf(out, "共%d个匹配函数\n", len(items))
		return nil
	}
	if len(items) == 0 {
		fmt.Fprintln(out, "没有符合前缀及描述的函数，无需删除。")
		return nil
	}
	fmt.Fprintf(out, "以上%d个函数将被删除（仅匹配前缀且Description=scfproxy）。\n", len(items))
	if err = confirm(in, out, interactive, yes); err != nil {
		return err
	}
	results := manager.Delete(ctx, items, *prefix)
	var urls []string
	failed := 0
	for _, r := range results {
		if r.Err != nil {
			failed++
			fmt.Fprintf(out, "删除失败 %s %s: %v\n", r.Region, r.Function, deploy.SafeError(r.Err))
		} else {
			fmt.Fprintf(out, "已删除 %s %s\n", r.Region, r.Function)
			if r.URL != "" {
				urls = append(urls, r.URL)
			} else {
				fmt.Fprintln(out, "  未能获取函数URL，请手动检查endpoints文件中的对应记录。")
			}
		}
	}
	if err = deploy.RemoveEndpoints(path, urls); err != nil {
		return err
	}
	fmt.Fprintf(out, "汇总: 删除成功%d 失败%d\n", len(results)-failed, failed)
	if failed > 0 {
		return errors.New("部分函数删除失败")
	}
	return nil
}
func probe(ctx context.Context, command string, args []string, out io.Writer) error {
	fs := flags(command, out)
	path := fs.String("f", "endpoints.txt", "每行一个ws/wss地址的文件")
	var target, download, upload, size string
	concurrency := 4
	var hold time.Duration
	noLimit := false
	if command == "check" {
		fs.StringVar(&target, "target", "https://api.ipify.org", "HTTP/HTTPS检测目标")
		fs.IntVar(&concurrency, "c", 4, "并发检测数")
	} else {
		fs.StringVar(&download, "download", "", "下载URL，默认阿里云Ubuntu镜像，最多读取--bytes字节")
		fs.StringVar(&upload, "upload", "", "上传POST目标，未指定则跳过上传")
		fs.StringVar(&size, "bytes", "10MB", "上传字节数和默认下载大小，支持MB/MiB")
		fs.BoolVar(&noLimit, "no-rate-limit", false, "关闭客户端上行默认限速")
		fs.DurationVar(&hold, "hold", 0, "到download目标的隧道空闲时长，仅靠WS Ping保活，之后同连接发请求")
	}
	if err := parse(fs, args); err != nil {
		return err
	}
	token := os.Getenv("SCFPROXY_TOKEN")
	if token == "" {
		return errors.New("缺少 SCFPROXY_TOKEN，请使用 scfproxy gen-token 生成")
	}
	endpoints, err := deploy.ReadEndpoints(*path)
	if err != nil {
		return err
	}
	if len(endpoints) == 0 {
		return errors.New("endpoints文件中没有地址")
	}
	failed := 0
	if command == "check" {
		if concurrency < 1 {
			return errors.New("并发数必须大于0")
		}
		for _, r := range deploy.Check(ctx, deploy.CheckOptions{Endpoints: endpoints, Token: token, Target: target, Concurrency: concurrency}) {
			fmt.Fprintf(out, "%s 建连+握手=%s 首字节(含建连/TLS)=%s HTTP=%d 正文=%q", r.Endpoint, r.DialDuration, r.FirstByteDuration, r.Status, r.Body)
			if r.Err != nil {
				failed++
				fmt.Fprintf(out, " 错误=%v", deploy.SafeError(r.Err))
			}
			fmt.Fprintln(out)
		}
	} else {
		n, err := deploy.ParseBytes(size)
		if err != nil {
			return err
		}
		if hold < 0 {
			return errors.New("hold不能为负数")
		}
		if upload == "" {
			fmt.Fprintln(out, "未指定 --upload，跳过上传测试。请提供可信的上传POST目标。 ")
		}
		fmt.Fprintf(out, "测速: bytes=%d no-rate-limit=%v hold=%s（hold目标为download URL，耗时包含空闲等待）\n", n, noLimit, hold)
		for _, r := range deploy.Bench(ctx, deploy.BenchOptions{Endpoints: endpoints, Token: token, DownloadURL: download, UploadURL: upload, Bytes: n, NoRateLimit: noLimit, Hold: hold}) {
			rate := float64(0)
			if r.Duration > 0 {
				rate = float64(r.Bytes) / r.Duration.Seconds()
			}
			fmt.Fprintf(out, "%s 字节=%d 耗时=%s 速率=%.2f B/s", r.Operation, r.Bytes, r.Duration, rate)
			if r.Warning != nil {
				fmt.Fprintf(out, " 警告=%v", deploy.SafeError(r.Warning))
			}
			if r.Err != nil {
				failed++
				fmt.Fprintf(out, " 错误=%v", deploy.SafeError(r.Err))
			}
			fmt.Fprintln(out)
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d项检测失败", failed)
	}
	return nil
}
