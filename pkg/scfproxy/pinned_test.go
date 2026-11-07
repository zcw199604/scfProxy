package scfproxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/zcw199604/scfproxy/internal/poolfile"
)

func writePinnedPool(t *testing.T, urls ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pool.json")
	if err := poolfile.Update(path, func(s *poolfile.State) error {
		for i, u := range urls {
			s.Active = append(s.Active, poolfile.Endpoint{URL: u, Region: []string{"ap-singapore", "ap-tokyo", "ap-seoul"}[i], Function: []string{"scfproxy-a", "scfproxy-b", "scfproxy-c"}[i], EgressIP: []string{"192.0.2.1", "192.0.2.2", "192.0.2.3"}[i], CreatedAt: time.Now()})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return path
}
func pinnedProxy(t *testing.T, path string, change func(*Config)) *Proxy {
	t.Helper()
	cfg := Config{PoolFile: path, Token: testToken, DisableRateLimit: true, PingInterval: -1, ReloadInterval: 10 * time.Millisecond}
	if change != nil {
		change(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}
func TestPinnedFailureNeverUsesStandby(t *testing.T) {
	var attempts atomic.Int64
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { attempts.Add(1); http.Error(w, "down", 503) }))
	defer bad.Close()
	good, count := newTunnelServer(t, true)
	p := pinnedProxy(t, writePinnedPool(t, "ws"+strings.TrimPrefix(bad.URL, "http"), good), nil)
	if p.cfg.Strategy != Pinned {
		t.Fatal("PoolFile 未强制 Pinned")
	}
	for n := 0; n < 2; n++ {
		if c, err := p.DialContext(context.Background(), "tcp", "127.0.0.1:1234"); err == nil {
			c.Close()
			t.Fatal("主用不可用却成功")
		}
	}
	if count.Load() != 0 || attempts.Load() != 6 {
		t.Fatalf("主用重试或备用隔离错误: main=%d standby=%d", attempts.Load(), count.Load())
	}
}
func TestPinnedSwitchClosesOldConnectionsAndPrewarm(t *testing.T) {
	main, mainCount := newTunnelServer(t, true)
	standby, standbyCount := newTunnelServer(t, true)
	callback := make(chan string, 1)
	release := make(chan struct{})
	defer close(release)
	p := pinnedProxy(t, writePinnedPool(t, main, standby), func(c *Config) {
		c.BanTTL = 6 * time.Hour
		c.Prewarm = 1
		c.OnSwitch = func(old, new EndpointInfo, reason string) { callback <- reason; <-release }
	})
	target := tcpEcho(t)
	c, err := p.DialContext(context.Background(), "tcp", target)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	eventually(t, func() bool {
		p.mu.Lock()
		child := p.pinned.child
		p.mu.Unlock()
		child.mu.Lock()
		defer child.mu.Unlock()
		return len(child.pool) == 1
	})
	before := mainCount.Load()
	switched := make(chan error, 1)
	go func() {
		old, next, err := p.Switch("blocked")
		if old.URL != main || next.URL != standby {
			err = errors.New("返回主备不正确")
		}
		switched <- err
	}()
	select {
	case err := <-switched:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Switch 被 OnSwitch 阻塞")
	}
	select {
	case reason := <-callback:
		if reason != "blocked" {
			t.Fatal(reason)
		}
	case <-time.After(time.Second):
		t.Fatal("没有回调")
	}
	c.SetReadDeadline(time.Now().Add(time.Second))
	if _, err = c.Read(make([]byte, 1)); err == nil {
		t.Fatal("旧连接未关闭")
	}
	fresh, err := p.DialContext(context.Background(), "tcp", target)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	fresh.SetDeadline(time.Now().Add(time.Second))
	fresh.Write([]byte("new"))
	got := make([]byte, 3)
	if _, err = io.ReadFull(fresh, got); err != nil || string(got) != "new" {
		t.Fatalf("new=%q err=%v", got, err)
	}
	if standbyCount.Load() == 0 {
		t.Fatal("新连接未使用备用")
	}
	time.Sleep(30 * time.Millisecond)
	if mainCount.Load() != before {
		t.Fatal("切换后仍预热旧主用")
	}
	state, err := poolfile.Read(p.cfg.PoolFile)
	if err != nil || len(state.Retired) != 1 || len(state.BannedIPs) != 1 {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	if state.BannedIPs[0].ExpiresAt.Sub(state.BannedIPs[0].BannedAt) != 6*time.Hour {
		t.Fatalf("Switch未使用Config.BanTTL: %+v", state.BannedIPs[0])
	}
}
func TestPinnedNoStandbyDoesNotChangeState(t *testing.T) {
	endpoint, _ := newTunnelServer(t, true)
	p := pinnedProxy(t, writePinnedPool(t, endpoint), nil)
	before := p.Active()
	if _, _, err := p.Switch("blocked"); !errors.Is(err, ErrNoStandby) {
		t.Fatalf("err=%v", err)
	}
	state, err := poolfile.Read(p.cfg.PoolFile)
	if err != nil || len(state.Active) != 1 || len(state.Retired) != 0 || len(state.BannedIPs) != 0 || p.Active() != before {
		t.Fatalf("state=%+v err=%v", state, err)
	}
}

func TestPinnedSwitchNegativeBanTTLSkipsBan(t *testing.T) {
	main, _ := newTunnelServer(t, true)
	standby, _ := newTunnelServer(t, true)
	p := pinnedProxy(t, writePinnedPool(t, main, standby), func(c *Config) { c.BanTTL = -1 })
	if _, _, err := p.Switch("manual"); err != nil {
		t.Fatal(err)
	}
	s, err := poolfile.Read(p.cfg.PoolFile)
	if err != nil || len(s.BannedIPs) != 0 || len(s.Retired) != 1 {
		t.Fatalf("state=%+v err=%v", s, err)
	}
}
func TestPinnedExternalReloadSwitchesAndClosesOld(t *testing.T) {
	main, _ := newTunnelServer(t, true)
	standby, _ := newTunnelServer(t, true)
	path := writePinnedPool(t, main, standby)
	callback := make(chan string, 1)
	p := pinnedProxy(t, path, func(c *Config) { c.OnSwitch = func(_, _ EndpointInfo, reason string) { callback <- reason } })
	c, err := p.DialContext(context.Background(), "tcp", tcpEcho(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, err = poolfile.Switch(path, "cli"); err != nil {
		t.Fatal(err)
	}
	select {
	case reason := <-callback:
		if reason != "external" {
			t.Fatal(reason)
		}
	case <-time.After(time.Second):
		t.Fatal("未热加载")
	}
	if p.Active().URL != standby {
		t.Fatal("未切换备用")
	}
	c.SetReadDeadline(time.Now().Add(time.Second))
	if _, err = c.Read(make([]byte, 1)); err == nil {
		t.Fatal("旧连接未关闭")
	}
}
func TestPinnedPoolConfigValidation(t *testing.T) {
	endpoint, _ := newTunnelServer(t, true)
	path := writePinnedPool(t, endpoint)
	if p, err := New(Config{PoolFile: path, Endpoints: []string{endpoint}, Token: testToken}); err == nil {
		p.Close()
		t.Fatal("PoolFile和Endpoints必须互斥")
	}
	if p, err := New(Config{PoolFile: path, EndpointsFile: "endpoints.txt", Token: testToken}); err == nil {
		p.Close()
		t.Fatal("PoolFile和EndpointsFile必须互斥")
	}
	if p, err := New(Config{PoolFile: filepath.Join(t.TempDir(), "missing.json"), Token: testToken}); err == nil {
		p.Close()
		t.Fatal("空池不能创建客户端")
	}
}

func TestPinnedReloadStandbyChangePreservesMainConnection(t *testing.T) {
	endpoint, _ := newTunnelServer(t, true)
	path := writePinnedPool(t, endpoint)
	callback := make(chan string, 1)
	p := pinnedProxy(t, path, func(c *Config) { c.OnSwitch = func(_, _ EndpointInfo, reason string) { callback <- reason } })
	c, err := p.DialContext(context.Background(), "tcp", tcpEcho(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	standby, _ := newTunnelServer(t, true)
	if err = poolfile.Update(path, func(s *poolfile.State) error {
		s.Active = append(s.Active, poolfile.Endpoint{URL: standby, Region: "ap-tokyo", Function: "scfproxy-b", EgressIP: "192.0.2.2", CreatedAt: time.Now()})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	c.SetDeadline(time.Now().Add(time.Second))
	c.Write([]byte("still-main"))
	got := make([]byte, 10)
	if _, err = io.ReadFull(c, got); err != nil || string(got) != "still-main" {
		t.Fatalf("仅修改备用关闭了主用: %q %v", got, err)
	}
	select {
	case <-callback:
		t.Fatal("主用未变却触发切换回调")
	default:
	}
}
func TestPinnedExternalEmptyPoolStopsDialAndClosesOld(t *testing.T) {
	endpoint, _ := newTunnelServer(t, true)
	path := writePinnedPool(t, endpoint)
	p := pinnedProxy(t, path, nil)
	c, err := p.DialContext(context.Background(), "tcp", tcpEcho(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = poolfile.Update(path, func(s *poolfile.State) error { s.Active = nil; return nil }); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return p.Active().URL == "" })
	c.SetReadDeadline(time.Now().Add(time.Second))
	if _, err = c.Read(make([]byte, 1)); err == nil {
		t.Fatal("清空池后旧连接未关闭")
	}
	if fresh, err := p.DialContext(context.Background(), "tcp", "127.0.0.1:1234"); err == nil {
		fresh.Close()
		t.Fatal("空池仍能拨号")
	}
}

func TestPinnedSwitchRetriesInFlightDialOnNewPrimary(t *testing.T) {
	received := make(chan struct{})
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		if _, _, err = ws.Read(r.Context()); err != nil {
			return
		}
		close(received)
		// 不回复握手；切换必须取消这一代连接，而不能让旧拨号晚到后交付。
		ws.Read(r.Context())
	}))
	defer old.Close()
	freshEndpoint, freshCount := newTunnelServer(t, true)
	p := pinnedProxy(t, writePinnedPool(t, "ws"+strings.TrimPrefix(old.URL, "http"), freshEndpoint), nil)
	target := tcpEcho(t)
	result := make(chan error, 1)
	go func() {
		c, err := p.DialContext(context.Background(), "tcp", target)
		if err == nil {
			defer c.Close()
			c.SetDeadline(time.Now().Add(time.Second))
			if _, err = c.Write([]byte("fresh")); err == nil {
				got := make([]byte, 5)
				if _, err = io.ReadFull(c, got); err == nil && string(got) != "fresh" {
					err = errors.New("新主用转发内容不正确")
				}
			}
		}
		result <- err
	}()
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("未收到旧代握手")
	}
	if _, _, err := p.Switch("blocked"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("切换期间的Dial未自动使用新主用: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("切换期间的Dial未完成")
	}
	if freshCount.Load() != 1 {
		t.Fatalf("新主用连接数=%d", freshCount.Load())
	}
}
