package scfproxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	gorilla "github.com/gorilla/websocket"
	"github.com/zcw199604/scfproxy/internal/server"
)

func TestConnDeadlineAndConcurrentClose(t *testing.T) {
	endpoint, _ := newTunnelServer(t, true)
	target := tcpEcho(t)
	p := newTestProxy(t, endpoint, nil)
	c, err := p.DialContext(context.Background(), "tcp", target)
	if err != nil {
		t.Fatal(err)
	}
	if c.LocalAddr() == nil || c.RemoteAddr() == nil {
		t.Fatal("地址不能为空")
	}
	c.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	_, err = c.Read(make([]byte, 1))
	var nerr net.Error
	if !errors.As(err, &nerr) || !nerr.Timeout() {
		t.Fatalf("读超时 err=%v", err)
	}
	c.SetReadDeadline(time.Time{})
	if _, err = c.Write([]byte("after deadline")); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, len("after deadline"))
	c.SetReadDeadline(time.Now().Add(time.Second))
	if _, err = io.ReadFull(c, data); err != nil || string(data) != "after deadline" {
		t.Fatalf("超时后连接不可用 data=%q err=%v", data, err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.Close() }()
	}
	wg.Wait()
}

func TestSmallWriteEventuallySentAndCloseFlushes(t *testing.T) {
	endpoint, _ := newTunnelServer(t, true)
	target := tcpEcho(t)
	p := newTestProxy(t, endpoint, nil)
	c, err := p.DialContext(context.Background(), "tcp", target)
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(time.Second))
	if _, err = c.Write([]byte("small")); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 5)
	if _, err = io.ReadFull(c, data); err != nil || string(data) != "small" {
		t.Fatalf("小包未按期发出 %q %v", data, err)
	}
	c.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	received := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		b := make([]byte, 11)
		n, _ := io.ReadFull(conn, b)
		received <- b[:n]
	}()
	flushProxy := newTestProxy(t, endpoint, func(c *Config) { c.CoalesceDelay = time.Hour })
	c, err = flushProxy.DialContext(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Write([]byte("close flush")); err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case b := <-received:
		if string(b) != "close flush" {
			t.Fatalf("关闭丢失缓冲数据 %q", b)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("关闭未 flush")
	}
}

func TestWriteDeadlineWhileRateLimited(t *testing.T) {
	endpoint, _ := newTunnelServer(t, true)
	target := tcpEcho(t)
	p := newTestProxy(t, endpoint, func(c *Config) { c.DisableRateLimit = false; c.MaxMessagesPerSecond = 1; c.MaxMessageSize = 1 })
	c, err := p.DialContext(context.Background(), "tcp", target)
	if err != nil {
		t.Fatal(err)
	}
	c.SetWriteDeadline(time.Now().Add(30 * time.Millisecond))
	_, err = c.Write(make([]byte, 128))
	var nerr net.Error
	if !errors.As(err, &nerr) || !nerr.Timeout() {
		t.Fatalf("写超时 err=%v", err)
	}
	c.Close()
}

func TestPrewarmPingBeforeTargetHandshake(t *testing.T) {
	var pings atomic.Int64
	var mu sync.Mutex
	active := map[*gorilla.Conn]bool{}
	up := gorilla.Upgrader{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		active[c] = true
		mu.Unlock()
		defer func() { c.Close(); mu.Lock(); delete(active, c); mu.Unlock() }()
		c.SetPingHandler(func(s string) error {
			pings.Add(1)
			return c.WriteControl(gorilla.PongMessage, []byte(s), time.Now().Add(time.Second))
		})
		typ, _, err := c.ReadMessage()
		if err != nil {
			return
		}
		if typ != gorilla.TextMessage {
			return
		}
		if c.WriteMessage(gorilla.TextMessage, []byte(`{"ok":true}`)) != nil {
			return
		}
		for {
			typ, b, err := c.ReadMessage()
			if err != nil {
				return
			}
			if c.WriteMessage(typ, b) != nil {
				return
			}
		}
	}))
	defer func() {
		mu.Lock()
		for c := range active {
			c.Close()
		}
		mu.Unlock()
		s.Close()
	}()
	p := newTestProxy(t, "ws"+strings.TrimPrefix(s.URL, "http"), func(c *Config) { c.Prewarm = 1; c.PingInterval = 10 * time.Millisecond })
	eventually(t, func() bool { return pings.Load() >= 2 })
	c, err := p.DialContext(context.Background(), "tcp", "unused.test:1234")
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(time.Second))
	c.Write([]byte("ping alive"))
	b := make([]byte, 10)
	if _, err = io.ReadFull(c, b); err != nil || string(b) != "ping alive" {
		t.Fatalf("预热保活后握手失败 %q %v", b, err)
	}
	c.Close()
	p.Close()
}

func TestEndpointsFileAndBypassRules(t *testing.T) {
	path := filepath.Join(t.TempDir(), "endpoints.txt")
	if err := os.WriteFile(path, []byte("# 注释\n  \n ws://127.0.0.1:1234/function/path \n"), 0600); err != nil {
		t.Fatal(err)
	}
	p, err := New(Config{Token: testToken, Endpoints: []string{"ws://127.0.0.1:1233/"}, EndpointsFile: path, Bypass: []string{"localhost", "*.internal", "10.0.0.0/8", "2001:db8::/32", "192.0.2.1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if len(p.endpoints) != 2 || p.endpoints[1] != "ws://127.0.0.1:1234/function/path" {
		t.Fatalf("文件未正确合并 endpoints=%v", p.endpoints)
	}
	for _, tt := range []struct {
		host string
		want bool
	}{
		{"localhost", true}, {"LOCALHOST.", true}, {"localhost.example", false},
		{"api.internal", true}, {"deep.api.internal", true}, {"internal", false}, {"api.internal.example", false},
		{"10.2.3.4", true}, {"11.2.3.4", false}, {"2001:db8::1", true}, {"2001:db9::1", false},
		{"192.0.2.1", true}, {"192.0.2.2", false},
	} {
		t.Run(tt.host, func(t *testing.T) {
			if got := p.bypass(tt.host); got != tt.want {
				t.Fatalf("bypass=%v want=%v", got, tt.want)
			}
		})
	}
}

func TestPrewarmMaxIdleReplacesConnection(t *testing.T) {
	endpoint, count := newTunnelServer(t, true)
	p := newTestProxy(t, endpoint, func(c *Config) { c.Prewarm = 1; c.PrewarmMaxIdle = 50 * time.Millisecond })
	eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return len(p.pool) == 1 })
	p.mu.Lock()
	first := p.pool[0]
	p.mu.Unlock()
	eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return len(p.pool) == 1 && p.pool[0] != first })
	if count.Load() < 2 {
		t.Fatal("达到最大闲置寿命后未重建")
	}
	p.Close()
	select {
	case <-first.done:
	case <-time.After(time.Second):
		t.Fatal("过期的预热连接未关闭")
	}
}

func TestRandomPrefersAvailablePrewarmEndpoint(t *testing.T) {
	endpoint, _ := newTunnelServer(t, true)
	p := newTestProxy(t, endpoint, func(c *Config) { c.Endpoints = []string{endpoint, endpoint}; c.Prewarm = 1 })
	eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return len(p.pool) == 1 })
	p.mu.Lock()
	endpointIndex := p.pool[0].endpoint
	p.mu.Unlock()
	for i := 0; i < 32; i++ {
		if got := p.order("example.test")[0]; got != endpointIndex {
			t.Fatalf("已有预热连接却选择冷出口: got=%d warm=%d", got, endpointIndex)
		}
	}
}

func TestRateLimitWaitCoalescesSmallWrites(t *testing.T) {
	endpoint, _ := newTunnelServer(t, true)
	observed, messages, _ := messageObserver(t, endpoint, 8192)
	p := newTestProxy(t, observed, nil)
	// 单独测试消息限速，字节限速保持关闭。
	p.cfg.MaxMessagesPerSecond = 10
	c, err := p.DialContext(context.Background(), "tcp", tcpEcho(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	data := bytes.Repeat([]byte("0123456789"), 30)
	start := time.Now()
	for i := 0; i < 30; i++ {
		if _, err = c.Write(data[i*10 : (i+1)*10]); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := make([]byte, len(data))
	if _, err = io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if !bytes.Equal(data, got) {
		t.Fatal("合并后数据不完整")
	}
	t.Logf("30 次小写入: elapsed=%v messages=%d", elapsed, messages.Load())
	if elapsed >= 1500*time.Millisecond {
		t.Errorf("限速等待未合并小包: %v", elapsed)
	}
	if n := messages.Load(); n > 6 {
		t.Errorf("小包未充分合并: %d 条消息", n)
	}
}

func TestClosedPrewarmRetriesSameEndpointWithoutCooldown(t *testing.T) {
	h, err := server.New(server.Config{Token: testToken, AllowPrivate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	var connections atomic.Int64
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if connections.Add(1) == 1 {
			ws, err := websocket.Accept(w, r, nil)
			if err == nil {
				ws.CloseNow()
			}
			return
		}
		h.ServeHTTP(w, r)
	}))
	defer s.Close()
	p := newTestProxy(t, "ws"+strings.TrimPrefix(s.URL, "http"), nil)
	// 手动安放真实已断开的预热连接，避免后台清理抢先移除而掩盖取用竞态。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	warm, err := p.connect(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-warm.done:
	case <-ctx.Done():
		t.Fatal("预热连接未被服务端关闭")
	}
	warm.cancel = func() {}
	warm.idleDone = make(chan struct{})
	close(warm.idleDone)
	p.mu.Lock()
	p.pool = append(p.pool, warm)
	p.mu.Unlock()
	c, err := p.DialContext(ctx, "tcp", tcpEcho(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	if _, err = c.Write([]byte("fresh")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	if _, err = io.ReadFull(c, got); err != nil || string(got) != "fresh" {
		t.Fatalf("新连接不可用: %q %v", got, err)
	}
	p.mu.Lock()
	cooldown := p.cooldown[0]
	p.mu.Unlock()
	if !cooldown.IsZero() {
		t.Fatalf("健康出口被错误冷却: %v", cooldown)
	}
	if n := connections.Load(); n != 2 {
		t.Fatalf("应仅重建一次连接: %d", n)
	}
}

func TestSingleEndpointRecoversDuringCooldown(t *testing.T) {
	h, err := server.New(server.Config{Token: testToken, AllowPrivate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	var ready atomic.Bool
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			http.Error(w, "暂时不可用", http.StatusServiceUnavailable)
			return
		}
		h.ServeHTTP(w, r)
	}))
	defer s.Close()
	p := newTestProxy(t, "ws"+strings.TrimPrefix(s.URL, "http"), func(c *Config) { c.EndpointCooldown = time.Minute })
	target := tcpEcho(t)
	if c, err := p.DialContext(context.Background(), "tcp", target); err == nil {
		c.Close()
		t.Fatal("不可用出口不应拨号成功")
	}
	p.mu.Lock()
	cooling := time.Now().Before(p.cooldown[0])
	p.mu.Unlock()
	if !cooling {
		t.Fatal("出口故障未进入冷却")
	}
	ready.Store(true)
	c, err := p.DialContext(context.Background(), "tcp", target)
	if err != nil {
		t.Fatalf("恢复后应立即成功: %v", err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	if _, err = c.Write([]byte("recovered")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 9)
	if _, err = io.ReadFull(c, got); err != nil || string(got) != "recovered" {
		t.Fatalf("恢复连接不可用: %q %v", got, err)
	}
}

func TestHandshakeCancellationDoesNotCoolEndpoint(t *testing.T) {
	for _, mode := range []string{"caller_cancel", "caller_deadline", "proxy_close", "dial_timeout"} {
		t.Run(mode, func(t *testing.T) {
			received := make(chan struct{})
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ws, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer ws.CloseNow()
				if _, _, err = ws.Read(r.Context()); err != nil {
					return
				}
				close(received)
				// 故意不回复握手，继续读取以响应客户端关闭。
				ws.Read(r.Context())
			}))
			defer s.Close()
			p := newTestProxy(t, "ws"+strings.TrimPrefix(s.URL, "http"), func(c *Config) {
				c.DialTimeout = 3 * time.Second
				if mode == "dial_timeout" {
					c.DialTimeout = 150 * time.Millisecond
				}
			})
			var ctx context.Context
			var cancel context.CancelFunc
			if mode == "caller_deadline" {
				ctx, cancel = context.WithTimeout(context.Background(), 150*time.Millisecond)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			result := make(chan error, 1)
			go func() {
				c, err := p.DialContext(ctx, "tcp", "unused.test:443")
				if c != nil {
					c.Close()
				}
				result <- err
			}()
			select {
			case <-received:
			case <-time.After(3 * time.Second):
				t.Fatal("未收到客户端握手")
			}
			if mode == "caller_cancel" {
				cancel()
			}
			if mode == "proxy_close" {
				p.Close()
			}
			var err error
			select {
			case err = <-result:
			case <-time.After(3 * time.Second):
				t.Fatal("取消后拨号未返回")
			}
			want := context.Canceled
			if mode == "caller_deadline" || mode == "dial_timeout" {
				want = context.DeadlineExceeded
			}
			if mode == "proxy_close" {
				if err == nil {
					t.Fatal("关闭 Proxy 后拨号应失败")
				}
			} else if !errors.Is(err, want) {
				t.Fatalf("err=%v want=%v", err, want)
			}
			p.mu.Lock()
			cooling := !p.cooldown[0].IsZero()
			p.mu.Unlock()
			if cooling != (mode == "dial_timeout") {
				t.Fatalf("冷却归因错误: mode=%s cooldown=%v err=%v", mode, cooling, err)
			}
		})
	}
}

func TestCooldownFallbackOrderAndRetryBudget(t *testing.T) {
	var mu sync.Mutex
	var attempts []string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts = append(attempts, r.URL.Path)
		mu.Unlock()
		http.Error(w, "暂时不可用", http.StatusServiceUnavailable)
	}))
	defer s.Close()
	endpoint := "ws" + strings.TrimPrefix(s.URL, "http")
	paths := []string{"/a", "/b", "/c", "/d"}
	p := newTestProxy(t, endpoint, func(c *Config) {
		c.Endpoints = []string{endpoint + paths[0], endpoint + paths[1], endpoint + paths[2], endpoint + paths[3]}
		c.Strategy = StickyByHost
		c.MaxRetries = 2
	})
	initial := p.order("unused.test")
	p.mu.Lock()
	p.cooldown[initial[1]] = time.Now().Add(time.Minute)
	p.cooldown[initial[3]] = time.Now().Add(2 * time.Minute)
	p.mu.Unlock()
	if c, err := p.DialContext(context.Background(), "tcp", "unused.test:443"); err == nil {
		c.Close()
		t.Fatal("不可用出口不应拨号成功")
	}
	want := strings.Join([]string{paths[initial[0]], paths[initial[2]], paths[initial[1]]}, ",")
	mu.Lock()
	got := strings.Join(attempts, ",")
	mu.Unlock()
	if got != want {
		t.Fatalf("应先尝试健康出口，再尝试最早到期的冷却出口，且最多三次: got=%s want=%s", got, want)
	}
}
