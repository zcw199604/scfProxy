package scfpool

import (
	"context"
	"fmt"
	scf "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/scf/v20180416"
	"github.com/zcw199604/scfproxy/internal/deploy"
	"github.com/zcw199604/scfproxy/internal/poolfile"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewManagerDefaultsAndEmptyStatus(t *testing.T) {
	m, err := NewManager(nil, Options{PoolFile: filepath.Join(t.TempDir(), "pool.json")})
	if err != nil {
		t.Fatal(err)
	}
	if m.opts.Memory != 64 {
		t.Fatalf("默认内存=%dMB，期望64MB", m.opts.Memory)
	}
	if m.opts.MaxConcurrency != 10 {
		t.Fatalf("默认并发=%d，期望10", m.opts.MaxConcurrency)
	}
	s, err := m.Status(context.Background())
	if err != nil || s.Version != 1 || len(s.Active) != 0 {
		t.Fatalf("s=%+v err=%v", s, err)
	}
	if err = m.Init(context.Background()); err == nil {
		t.Fatal("没有地域和凭证时不能初始化")
	}
}

func ptr[T any](v T) *T { return &v }

type cloud struct {
	mu           sync.Mutex
	events       []string
	clients      map[string]*api
	ips          map[string][]string
	beforeCreate func()
	description  string
}
type api struct {
	deploy.SCFAPI
	c                *cloud
	region, name, ip string
	concurrency      *scf.InstanceConcurrencyConfig
}

func (c *cloud) factory(region string) (deploy.SCFAPI, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clients == nil {
		c.clients = map[string]*api{}
	}
	a := c.clients[region]
	if a == nil {
		a = &api{c: c, region: region}
		c.clients[region] = a
	}
	return a, nil
}
func (a *api) CreateFunctionWithContext(_ context.Context, r *scf.CreateFunctionRequest) (*scf.CreateFunctionResponse, error) {
	if a.c.beforeCreate != nil {
		a.c.beforeCreate()
	}
	a.c.mu.Lock()
	defer a.c.mu.Unlock()
	a.name = *r.FunctionName
	a.concurrency = r.InstanceConcurrencyConfig
	a.c.events = append(a.c.events, "create:"+a.region)
	list := a.c.ips[a.region]
	if len(list) == 0 {
		a.ip = "192.0.2." + map[string]string{"ap-a": "1", "ap-b": "2", "ap-c": "3"}[a.region]
	} else {
		a.ip = list[0]
		if len(list) > 1 {
			a.c.ips[a.region] = list[1:]
		}
	}
	return &scf.CreateFunctionResponse{}, nil
}

func TestPoolMaxConcurrencyForwardingAndValidation(t *testing.T) {
	c := &cloud{}
	o := opts(t)
	o.MaxConcurrency = 7
	m, err := NewManager(c.factory, o)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, a := range c.clients {
		if a.concurrency == nil || *a.concurrency.MaxConcurrency != 7 || *a.concurrency.DynamicEnabled != "FALSE" || *a.concurrency.Type != "Request-Based" {
			t.Fatalf("未传递并发配置: %+v", a.concurrency)
		}
	}
	for _, n := range []int{-1, 101} {
		o.MaxConcurrency = n
		if _, err = NewManager(c.factory, o); err == nil {
			t.Fatalf("接受无效并发数%d", n)
		}
	}
}

func TestPoolDefaultAndDisabledConcurrency(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprint(disabled), func(t *testing.T) {
			c := &cloud{}
			o := opts(t)
			o.DisableConcurrency = disabled
			m, err := NewManager(c.factory, o)
			if err != nil {
				t.Fatal(err)
			}
			if err = m.Init(context.Background()); err != nil {
				t.Fatal(err)
			}
			for _, a := range c.clients {
				if disabled {
					if a.concurrency != nil {
						t.Fatalf("显式关闭仍发送并发配置: %+v", a.concurrency)
					}
				} else if a.concurrency == nil || *a.concurrency.MaxConcurrency != 10 {
					t.Fatalf("默认并发不是10: %+v", a.concurrency)
				}
			}
		})
	}
}
func (a *api) GetFunctionWithContext(_ context.Context, _ *scf.GetFunctionRequest) (*scf.GetFunctionResponse, error) {
	a.c.mu.Lock()
	defer a.c.mu.Unlock()
	desc := a.c.description
	if desc == "" {
		desc = "scfproxy"
	}
	return &scf.GetFunctionResponse{Response: &scf.GetFunctionResponseParams{Status: ptr("Active"), Description: ptr(desc), PublicNetConfig: &scf.PublicNetConfigOut{EipConfig: &scf.EipConfigOut{EipAddress: []*string{ptr(a.ip)}}}}}, nil
}
func (a *api) ListTriggersWithContext(context.Context, *scf.ListTriggersRequest) (*scf.ListTriggersResponse, error) {
	return &scf.ListTriggersResponse{Response: &scf.ListTriggersResponseParams{Triggers: []*scf.TriggerInfo{{Type: ptr("http"), Qualifier: ptr("$LATEST"), TriggerDesc: ptr(`{"NetConfig":{"ExtranetUrl":"https://` + a.region + `.example.test"}}`)}}}}, nil
}
func (a *api) DeleteFunctionWithContext(_ context.Context, r *scf.DeleteFunctionRequest) (*scf.DeleteFunctionResponse, error) {
	a.c.mu.Lock()
	defer a.c.mu.Unlock()
	a.c.events = append(a.c.events, "delete:"+a.region)
	return &scf.DeleteFunctionResponse{}, nil
}
func opts(t *testing.T) Options {
	return Options{PoolFile: filepath.Join(t.TempDir(), "pool.json"), Regions: []string{"ap-a", "ap-b", "ap-c"}, Size: 2, Token: "test-token", Package: []byte("test-package"), EgressIPTimeout: time.Second}
}

func TestConflictingIgnoresExpiredBans(t *testing.T) {
	now := time.Now()
	e := Endpoint{Region: "ap-a", EgressIP: "192.0.2.99"}
	for _, tc := range []struct {
		name     string
		expires  time.Time
		conflict bool
	}{
		{"expired", now.Add(-time.Hour), false},
		{"unexpired", now.Add(time.Hour), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := State{BannedIPs: []Ban{{IP: e.EgressIP, BannedAt: now.Add(-2 * time.Hour), ExpiresAt: tc.expires}}}
			if conflicting(s, e) != tc.conflict {
				t.Fatalf("conflicting=%v", !tc.conflict)
			}
		})
	}
}

func TestManagerSwitchUsesBanTTL(t *testing.T) {
	c := &cloud{}
	o := opts(t)
	o.BanTTL = 30 * time.Minute
	m, err := NewManager(c.factory, o)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.Switch("blocked"); err != nil {
		t.Fatal(err)
	}
	s, err := m.Status(context.Background())
	if err != nil || len(s.BannedIPs) != 1 || s.BannedIPs[0].ExpiresAt.Sub(s.BannedIPs[0].BannedAt) != o.BanTTL {
		t.Fatalf("state=%+v err=%v", s, err)
	}
}
func TestInitDifferentRegionsAndReplenishDeletesFirst(t *testing.T) {
	c := &cloud{}
	o := opts(t)
	m, _ := NewManager(c.factory, o)
	if err := m.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	s, _ := m.Status(context.Background())
	if len(s.Active) != 2 || s.Active[0].Region == s.Active[1].Region {
		t.Fatalf("state=%+v", s)
	}
	Switch(o.PoolFile, "banned")
	report, err := m.Replenish(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Deleted) != 1 || len(report.Created) != 1 || report.Created[0].Region != "ap-c" {
		t.Fatalf("report=%+v", report)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if strings.Join(c.events, ",") != "create:ap-a,create:ap-b,delete:ap-a,create:ap-c" {
		t.Fatal(c.events)
	}
}
func TestReplenishRejectsBannedAndDuplicateIP(t *testing.T) {
	c := &cloud{ips: map[string][]string{"ap-b": {"192.0.2.99"}, "ap-c": {"192.0.2.1", "192.0.2.3"}}}
	o := opts(t)
	o.MaxAttempts = 4
	poolfile.Update(o.PoolFile, func(s *State) error {
		s.BannedIPs = []Ban{{IP: "192.0.2.99", BannedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}}
		return nil
	})
	m, _ := NewManager(c.factory, o)
	if err := m.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	s, _ := m.Status(context.Background())
	if len(s.Active) != 2 || s.Active[1].EgressIP != "192.0.2.3" || len(s.Retired) != 0 {
		t.Fatalf("state=%+v", s)
	}
}
func TestReplenishConflictLimitAndOwnership(t *testing.T) {
	c := &cloud{ips: map[string][]string{"ap-a": {"192.0.2.99"}, "ap-b": {"192.0.2.99"}, "ap-c": {"192.0.2.99"}}}
	o := opts(t)
	o.MaxAttempts = 2
	poolfile.Update(o.PoolFile, func(s *State) error {
		s.BannedIPs = []Ban{{IP: "192.0.2.99", BannedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}}
		return nil
	})
	m, _ := NewManager(c.factory, o)
	if err := m.Init(context.Background()); err == nil {
		t.Fatal("超过IP冲突上限未报错")
	}
	s, _ := m.Status(context.Background())
	if len(s.Active) != 0 || len(s.Retired) != 0 {
		t.Fatalf("冲突未清理: %+v", s)
	}
	poolfile.Update(o.PoolFile, func(s *State) error {
		s.Retired = []Retired{{Endpoint: Endpoint{Region: "ap-a", Function: "scfproxy-ap-a-1"}}}
		return nil
	})
	c.description = "other"
	c.mu.Lock()
	c.events = nil
	c.mu.Unlock()
	if err := m.Destroy(context.Background()); err == nil {
		t.Fatal("不属于本工具仍删除")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.events) != 0 {
		t.Fatal(c.events)
	}
}
func TestConcurrentReplenishAndSwitchDoNotLoseState(t *testing.T) {
	c := &cloud{}
	o := opts(t)
	o.Size = 3
	m, _ := NewManager(c.factory, o)
	first := Endpoint{URL: "ws://ap-a.example.test", Region: "ap-a", Function: "scfproxy-ap-a-1", EgressIP: "192.0.2.1", CreatedAt: time.Now()}
	second := Endpoint{URL: "ws://ap-b.example.test", Region: "ap-b", Function: "scfproxy-ap-b-1", EgressIP: "192.0.2.2", CreatedAt: time.Now()}
	poolfile.Update(o.PoolFile, func(s *State) error { s.Active = []Endpoint{first, second}; return nil })
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	c.beforeCreate = func() { once.Do(func() { close(entered); <-release }) }
	done := make(chan error, 2)
	go func() { _, err := m.Replenish(context.Background()); done <- err }()
	<-entered
	if _, _, err := Switch(o.PoolFile, "manual"); err != nil {
		t.Fatal(err)
	}
	close(release)
	// 无第四个地域，补完一个后不足Size应报告无法补位，但切换和已创建状态都要保留。
	if err := <-done; err == nil {
		t.Fatal("地域不足应报错")
	}
	s, _ := m.Status(context.Background())
	if len(s.Active) != 2 || s.Active[0].Region != "ap-b" || len(s.Retired) != 1 || len(s.BannedIPs) != 1 {
		t.Fatalf("交错覆盖状态: %+v", s)
	}
	// 两个不同Manager同文件串行，后一个看见前一个结果，不会重复创建。
	o.Size = 2
	m1, _ := NewManager(c.factory, o)
	m2, _ := NewManager(c.factory, o)
	go func() { _, err := m1.Replenish(context.Background()); done <- err }()
	go func() { _, err := m2.Replenish(context.Background()); done <- err }()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s, _ = m.Status(context.Background())
	if len(s.Active) != 2 || len(s.Retired) != 0 {
		t.Fatalf("state=%+v", s)
	}
}

func TestDestroyClearsFunctionsButKeepsBannedIPs(t *testing.T) {
	c := &cloud{}
	o := opts(t)
	m, _ := NewManager(c.factory, o)
	if err := m.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Switch(o.PoolFile, "blocked"); err != nil {
		t.Fatal(err)
	}
	if err := m.Destroy(context.Background()); err != nil {
		t.Fatal(err)
	}
	s, err := m.Status(context.Background())
	if err != nil || len(s.Active) != 0 || len(s.Retired) != 0 || len(s.BannedIPs) != 1 {
		t.Fatalf("state=%+v err=%v", s, err)
	}
}
