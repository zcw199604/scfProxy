// Package scfpool 管理跨地域固定出口 IP 主备池，不会自动切换主用出口。
package scfpool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zcw199604/scfproxy/internal/deploy"
	"github.com/zcw199604/scfproxy/internal/poolfile"
)

type Factory = deploy.Factory
type SCFAPI = deploy.SCFAPI
type Endpoint = poolfile.Endpoint
type Retired = poolfile.Retired
type State = poolfile.State
type Ban = poolfile.Ban

var ErrNoStandby = poolfile.ErrNoStandby

func Read(path string) (State, error) { return poolfile.Read(path) }
func Switch(path, reason string, ttl ...time.Duration) (Endpoint, Endpoint, error) {
	return poolfile.Switch(path, reason, ttl...)
}
func Unban(path, ip string) error { return poolfile.Unban(path, ip) }

type Options struct {
	PoolFile                     string
	Regions                      []string
	Size                         int
	Prefix, Token                string
	Package                      []byte
	Runtime                      string
	Timeout, IdleTimeout, Memory int64
	MaxAttempts                  int
	EgressIPTimeout              time.Duration
	BanTTL                       time.Duration
	MaxConcurrency               int
	// DisableConcurrency 显式不设置多并发；否则 MaxConcurrency=0 使用默认10。
	DisableConcurrency bool
	Logger             *slog.Logger
}
type Change struct {
	Endpoint
	Duration time.Duration
}
type Report struct{ Deleted, Created []Change }
type Manager struct {
	factory deploy.Factory
	opts    Options
	mu      sync.Mutex
}

func NewManager(factory deploy.Factory, opts Options) (*Manager, error) {
	if opts.BanTTL == 0 {
		opts.BanTTL = poolfile.DefaultBanTTL
	}
	if opts.MaxConcurrency < 0 || opts.MaxConcurrency > 100 {
		return nil, errors.New("max-concurrency 必须在0~100之间")
	}
	if opts.DisableConcurrency {
		opts.MaxConcurrency = 0
	} else if opts.MaxConcurrency == 0 {
		opts.MaxConcurrency = 10
	}
	if opts.PoolFile == "" {
		return nil, errors.New("scfpool: PoolFile 不能为空")
	}
	if opts.Size == 0 {
		opts.Size = 2
	}
	if opts.Prefix == "" {
		opts.Prefix = "scfproxy"
	}
	if opts.Runtime == "" {
		opts.Runtime = "Go1"
	}
	if opts.Timeout == 0 {
		opts.Timeout = 900
	}
	if opts.IdleTimeout == 0 {
		opts.IdleTimeout = 600
	}
	if opts.Memory == 0 {
		opts.Memory = 64
	}
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = 3
	}
	if opts.EgressIPTimeout == 0 {
		opts.EgressIPTimeout = 3 * time.Minute
	}
	if opts.Size < 1 || opts.MaxAttempts < 1 || opts.Timeout < opts.IdleTimeout || opts.IdleTimeout < 1 || opts.Memory < 1 || opts.EgressIPTimeout < 0 {
		return nil, errors.New("scfpool: 配置数值无效")
	}
	if err := deploy.ValidatePrefix(opts.Prefix); err != nil {
		return nil, err
	}
	if len(opts.Regions) > 0 {
		regions, err := deploy.ResolveRegions(strings.Join(opts.Regions, ","), "")
		if err != nil {
			return nil, err
		}
		opts.Regions = regions
		for _, region := range regions {
			if _, err = deploy.FunctionName(opts.Prefix, region, int(time.Now().UnixNano())); err != nil {
				return nil, err
			}
		}
	}
	opts.Package = append([]byte(nil), opts.Package...)
	return &Manager{factory: factory, opts: opts}, nil
}

// Switch 使用管理器配置的封禁有效期，只执行本地池文件切换。
func (m *Manager) Switch(reason string) (Endpoint, Endpoint, error) {
	return Switch(m.opts.PoolFile, reason, m.opts.BanTTL)
}
func NewManagerFromEnv(opts Options) (*Manager, error) {
	factory, err := deploy.NewSDKFactoryFromEnv()
	if err != nil {
		return nil, err
	}
	return NewManager(factory, opts)
}
func (m *Manager) Status(ctx context.Context) (State, error) {
	if err := ctx.Err(); err != nil {
		return State{}, err
	}
	return Read(m.opts.PoolFile)
}
func (m *Manager) Init(ctx context.Context) error {
	_, err := m.run(ctx, true, false)
	return err
}
func (m *Manager) Replenish(ctx context.Context) (Report, error) { return m.run(ctx, false, false) }
func (m *Manager) Destroy(ctx context.Context) error             { _, err := m.run(ctx, false, true); return err }
func (m *Manager) run(ctx context.Context, init, destroy bool) (Report, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var report Report
	unlock, err := poolfile.Lock(ctx, m.opts.PoolFile+".manage.lock")
	if err != nil {
		return report, err
	}
	defer unlock()
	if err = ctx.Err(); err != nil {
		return report, err
	}
	state, err := Read(m.opts.PoolFile)
	if err != nil {
		return report, err
	}
	if init && len(state.Active) > 0 {
		return report, nil
	}
	if m.factory == nil {
		return report, errors.New("scfpool: 缺少云接口Factory")
	}
	if !destroy && (len(m.opts.Regions) < m.opts.Size || m.opts.Token == "" || len(m.opts.Package) == 0) {
		return report, errors.New("scfpool: 创建需要不少于Size的候选地域、Token和Package")
	}
	dm := &deploy.Manager{Factory: m.factory, EgressIPTimeout: m.opts.EgressIPTimeout}
	if destroy {
		err = poolfile.Update(m.opts.PoolFile, func(s *State) error {
			now := time.Now().UTC()
			for _, e := range s.Active {
				s.Retired = append(s.Retired, Retired{Endpoint: e, RetiredAt: now, Reason: "destroy"})
				s.RegionLastUsed[e.Region] = now
			}
			s.Active = nil
			return nil
		})
		if err != nil {
			return report, err
		}
	}
	state, err = Read(m.opts.PoolFile)
	if err != nil {
		return report, err
	}
	justDeleted := map[string]bool{}
	var failures []error
	for _, r := range state.Retired {
		start := time.Now()
		if err = m.remove(ctx, dm, r.Endpoint); err != nil {
			failures = append(failures, fmt.Errorf("删除 %s/%s: %w", r.Region, r.Function, err))
			continue
		}
		if err = m.forget(r.Endpoint); err != nil {
			return report, err
		}
		justDeleted[r.Region] = true
		report.Deleted = append(report.Deleted, Change{Endpoint: r.Endpoint, Duration: time.Since(start)})
	}
	if destroy {
		return report, errors.Join(failures...)
	}
	for {
		if err = ctx.Err(); err != nil {
			return report, errors.Join(append(failures, err)...)
		}
		state, err = Read(m.opts.PoolFile)
		if err != nil {
			return report, err
		}
		if len(state.Active) >= m.opts.Size {
			return report, errors.Join(failures...)
		}
		completed := false
		for attempt := 0; attempt < m.opts.MaxAttempts; attempt++ {
			var pending Endpoint
			err = poolfile.Update(m.opts.PoolFile, func(s *State) error {
				if len(s.Active) >= m.opts.Size {
					return nil
				}
				region := m.chooseRegion(*s, justDeleted)
				if region == "" {
					return errors.New("scfpool: 没有未占用的候选地域")
				}
				name, e := deploy.FunctionName(m.opts.Prefix, region, int(time.Now().UnixNano()))
				if e != nil {
					return e
				}
				now := time.Now().UTC()
				pending = Endpoint{Region: region, Function: name, CreatedAt: now}
				// 创建前登记意图，进程中断或建连超时后仍可找到待清理资源。
				s.Retired = append(s.Retired, Retired{Endpoint: pending, RetiredAt: now, Reason: "creating"})
				s.RegionLastUsed[region] = now
				return nil
			})
			if err != nil {
				return report, errors.Join(append(failures, err)...)
			}
			if pending.Function == "" {
				return report, errors.Join(failures...)
			}
			start := time.Now()
			o := deploy.Options{Regions: []string{pending.Region}, Count: 1, Prefix: m.opts.Prefix, Runtime: m.opts.Runtime, Token: m.opts.Token, Package: m.opts.Package, Timeout: m.opts.Timeout, IdleTimeout: m.opts.IdleTimeout, Memory: m.opts.Memory, Concurrency: 1, FixedIP: true, MaxConcurrency: m.opts.MaxConcurrency}
			result := dm.Create(ctx, o, deploy.Endpoint{Region: pending.Region, Function: pending.Function})
			if result.Existed {
				// 不接管同名既有函数，撤销本地意图后换名；不删除该远端资源。
				if err = m.forget(pending); err != nil {
					return report, err
				}
				continue
			}
			if result.Err != nil {
				return report, errors.Join(append(failures, fmt.Errorf("创建 %s/%s: %w", pending.Region, pending.Function, result.Err))...)
			}
			pending.URL = result.URL
			pending.EgressIP = result.EgressIP
			invalid := false
			err = poolfile.Update(m.opts.PoolFile, func(s *State) error {
				for i := range s.Retired {
					if same(s.Retired[i].Endpoint, pending) {
						s.Retired[i].Endpoint = pending
					}
				}
				if len(s.Active) >= m.opts.Size || conflicting(*s, pending) {
					invalid = true
					return nil
				}
				s.Retired = without(s.Retired, pending)
				s.Active = append(s.Active, pending)
				return nil
			})
			if err != nil {
				return report, err
			}
			if invalid {
				deleteStart := time.Now()
				if err = m.remove(ctx, dm, pending); err != nil {
					return report, errors.Join(append(failures, err)...)
				}
				if err = m.forget(pending); err != nil {
					return report, err
				}
				justDeleted[pending.Region] = true
				report.Deleted = append(report.Deleted, Change{Endpoint: pending, Duration: time.Since(deleteStart)})
				continue
			}
			report.Created = append(report.Created, Change{Endpoint: pending, Duration: time.Since(start)})
			completed = true
			if m.opts.Logger != nil {
				m.opts.Logger.Info("固定出口补位完成", "region", pending.Region, "function", pending.Function, "egress_ip", pending.EgressIP)
			}
			break
		}
		if !completed {
			return report, errors.Join(append(failures, fmt.Errorf("scfpool: 补位超过 %s 次尝试，可能仍分配到重复或已封IP", strconv.Itoa(m.opts.MaxAttempts)))...)
		}
	}
}
func (m *Manager) chooseRegion(s State, justDeleted map[string]bool) string {
	used := map[string]bool{}
	for _, e := range s.Active {
		used[e.Region] = true
	}
	for _, e := range s.Retired {
		used[e.Region] = true
	}
	var available []string
	for _, r := range m.opts.Regions {
		if !used[r] {
			available = append(available, r)
		}
	}
	sort.SliceStable(available, func(i, j int) bool {
		a, b := available[i], available[j]
		if justDeleted[a] != justDeleted[b] {
			return !justDeleted[a]
		}
		return s.RegionLastUsed[a].Before(s.RegionLastUsed[b])
	})
	if len(available) == 0 {
		return ""
	}
	return available[0]
}
func (m *Manager) remove(ctx context.Context, dm *deploy.Manager, e Endpoint) error {
	results := dm.Delete(ctx, []deploy.Result{{Endpoint: deploy.Endpoint{Region: e.Region, Function: e.Function, URL: e.URL}, Description: "scfproxy"}}, m.opts.Prefix)
	return results[0].Err
}
func (m *Manager) forget(e Endpoint) error {
	return poolfile.Update(m.opts.PoolFile, func(s *State) error {
		s.Retired = without(s.Retired, e)
		s.RegionLastUsed[e.Region] = time.Now().UTC()
		return nil
	})
}
func same(a, b Endpoint) bool { return a.Region == b.Region && a.Function == b.Function }
func without(list []Retired, e Endpoint) []Retired {
	kept := list[:0]
	for _, r := range list {
		if !same(r.Endpoint, e) {
			kept = append(kept, r)
		}
	}
	return kept
}
func conflicting(s State, e Endpoint) bool {
	ip, err := netip.ParseAddr(e.EgressIP)
	if err != nil {
		return true
	}
	ip = ip.Unmap()
	for _, a := range s.Active {
		other, _ := netip.ParseAddr(a.EgressIP)
		if a.Region == e.Region || other.Unmap() == ip {
			return true
		}
	}
	for _, b := range s.BannedIPs {
		if !b.ExpiresAt.After(time.Now()) {
			continue
		}
		other, _ := netip.ParseAddr(b.IP)
		if other.Unmap() == ip {
			return true
		}
	}
	return false
}
