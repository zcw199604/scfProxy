package deploy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/zcw199604/scfproxy/pkg/scfproxy"
)

// ConcurrencyOptions 仅探测一个已部署出口，不创建云资源。
type ConcurrencyOptions struct {
	Endpoint, Token, Target string
	Count                   int
	Hold, Stagger           time.Duration
}
type TunnelResult struct {
	DialDuration time.Duration
	Instance     string
	Active       int
	Connected    bool
	Err          error
}
type ConcurrencyReport struct {
	Tunnels                               []TunnelResult
	Instances                             map[string]int
	UnknownInstances, MaxActive, Failures int
	P50, MaxDial                          time.Duration
}

func Concurrency(ctx context.Context, o ConcurrencyOptions) (ConcurrencyReport, error) {
	report := ConcurrencyReport{Instances: map[string]int{}}
	if o.Count < 1 || o.Hold < 0 || o.Stagger < 0 {
		return report, errors.New("n必须大于0，hold和stagger不能为负数")
	}
	host, port, err := net.SplitHostPort(o.Target)
	if err != nil || host == "" || port == "" {
		return report, errors.New("target必须是host:port，使用明文HTTP HEAD保活")
	}
	p, err := scfproxy.New(scfproxy.Config{Endpoints: []string{o.Endpoint}, Token: o.Token})
	if err != nil {
		return report, probeError(err, o.Token)
	}
	defer p.Close()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	report.Tunnels = make([]TunnelResult, o.Count)
	var ready, done sync.WaitGroup
	ready.Add(o.Count)
	done.Add(o.Count)
	for i := 0; i < o.Count; i++ {
		if i > 0 && o.Stagger > 0 {
			_ = wait(runCtx, o.Stagger)
		}
		go func(i int) {
			defer done.Done()
			r := &report.Tunnels[i]
			start := time.Now()
			c, err := p.DialContext(runCtx, "tcp", o.Target)
			r.DialDuration = time.Since(start)
			if err != nil {
				r.Err = probeError(err, o.Token)
				ready.Done()
				return
			}
			r.Connected = true
			if info, ok := c.(scfproxy.TunnelInfo); ok {
				r.Instance = info.InstanceID()
				r.Active = info.InstanceActive()
			}
			defer c.Close()
			stop := context.AfterFunc(runCtx, func() { c.Close() })
			defer stop()
			ready.Done()
			reader := bufio.NewReader(c)
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-runCtx.Done():
					if ctx.Err() != nil {
						r.Err = ctx.Err()
					}
					return
				case <-ticker.C:
					err := concurrencyHead(c, reader, o.Target)
					if err != nil {
						if runCtx.Err() == nil {
							r.Err = probeError(fmt.Errorf("TCP保活失败: %w", err), o.Token)
						} else if ctx.Err() != nil {
							r.Err = ctx.Err()
						}
						return
					}
				}
			}
		}(i)
	}
	ready.Wait()
	// 所有拨号完成后才开始共同保持阶段；较早建立的连接已开始保活。
	holdErr := wait(runCtx, o.Hold)
	cancel()
	done.Wait()
	var durations []time.Duration
	for _, r := range report.Tunnels {
		if r.Err != nil {
			report.Failures++
		}
		if !r.Connected {
			continue
		}
		durations = append(durations, r.DialDuration)
		if r.Instance == "" {
			report.UnknownInstances++
		} else {
			report.Instances[r.Instance]++
		}
		report.MaxActive = max(report.MaxActive, r.Active)
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	if len(durations) > 0 {
		report.P50 = durations[(len(durations)-1)/2]
		report.MaxDial = durations[len(durations)-1]
	}
	return report, holdErr
}

// concurrencyHead 在同一TCP连接上读完HEAD响应头，不跟随重定向或建立其他连接。
func concurrencyHead(c net.Conn, reader *bufio.Reader, target string) error {
	if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	defer c.SetDeadline(time.Time{})
	req := &http.Request{Method: http.MethodHead, Host: target, URL: &url.URL{Path: "/"}, ProtoMajor: 1, ProtoMinor: 1, Header: make(http.Header)}
	if err := req.Write(c); err != nil {
		return err
	}
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.Close {
		return errors.New("目标要求关闭TCP连接，请选择支持HTTP keep-alive的明文HTTP目标")
	}
	return nil
}
