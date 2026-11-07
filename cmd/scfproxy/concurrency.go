package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/zcw199604/scfproxy/internal/deploy"
)

func concurrencyCommand(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("concurrency", out)
	path := fs.String("f", "endpoints.txt", "只测试文件中第一个endpoint")
	var o deploy.ConcurrencyOptions
	fs.IntVar(&o.Count, "n", 10, "同时建立的隧道数")
	fs.DurationVar(&o.Hold, "hold", 30*time.Second, "全部拨号完成后的保持时长")
	fs.StringVar(&o.Target, "target", "www.baidu.com:80", "TCP目标host:port，须支持明文HTTP HEAD和keep-alive")
	fs.DurationVar(&o.Stagger, "stagger", 0, "每条隧道开始拨号的间隔，0同时开始")
	if err := parse(fs, args); err != nil {
		return err
	}
	o.Token = os.Getenv("SCFPROXY_TOKEN")
	if o.Token == "" {
		return errors.New("缺少 SCFPROXY_TOKEN，请使用scfproxy gen-token生成")
	}
	endpoints, err := deploy.ReadEndpoints(*path)
	if err != nil {
		return err
	}
	if len(endpoints) == 0 {
		return errors.New("endpoints文件中没有地址")
	}
	o.Endpoint = endpoints[0]
	fmt.Fprintf(out, "测试endpoint=%s target=%s n=%d hold=%s stagger=%s；每10s发送HEAD保活，WS ping使用默认30s。\n", o.Endpoint, o.Target, o.Count, o.Hold, o.Stagger)
	r, err := deploy.Concurrency(ctx, o)
	for i, tunnel := range r.Tunnels {
		instance := tunnel.Instance
		if instance == "" {
			instance = "-"
		}
		fmt.Fprintf(out, "隧道%d 建连+握手=%s instance=%s active=%d", i+1, tunnel.DialDuration, instance, tunnel.Active)
		if tunnel.Err != nil {
			fmt.Fprintf(out, " 错误=%v", deploy.SafeError(tunnel.Err))
		}
		fmt.Fprintln(out)
	}
	var instances []string
	for id := range r.Instances {
		instances = append(instances, id)
	}
	sort.Strings(instances)
	for _, id := range instances {
		fmt.Fprintf(out, "实例%s 隧道数=%d\n", id, r.Instances[id])
	}
	fmt.Fprintf(out, "汇总: 不同实例数=%d 未报告实例的隧道=%d 最大active=%d 建连P50=%s 最大=%s 失败=%d\n", len(instances), r.UnknownInstances, r.MaxActive, r.P50, r.MaxDial, r.Failures)
	if err != nil {
		return err
	}
	if r.Failures > 0 {
		return errors.New("部分隧道建连或保活失败")
	}
	return nil
}
