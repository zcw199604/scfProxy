package scfproxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	gorilla "github.com/gorilla/websocket"
	"github.com/zcw199604/scfproxy/internal/server"
)

const testToken = "本地测试令牌"

func newTunnelServer(t *testing.T, allowPrivate bool) (string, *atomic.Int64) {
	t.Helper()
	h, err := server.New(server.Config{Token: testToken, AllowPrivate: allowPrivate, MaxMessageSize: 8192})
	if err != nil {
		t.Fatal(err)
	}
	var count atomic.Int64
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			count.Add(1)
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { h.Close(); s.Close() })
	return "ws" + strings.TrimPrefix(s.URL, "http"), &count
}

func newTestProxy(t *testing.T, endpoint string, change func(*Config)) *Proxy {
	t.Helper()
	cfg := Config{Token: testToken, Endpoints: []string{endpoint}, DisableRateLimit: true, PingInterval: -1, MaxMessageSize: 8192}
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

func TestE2EHTTPAndTLS(t *testing.T) {
	endpoint, _ := newTunnelServer(t, true)
	p := newTestProxy(t, endpoint, nil)
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "普通 HTTP 正文") }))
	defer plain.Close()
	tr := p.Transport()
	defer tr.CloseIdleConnections()
	c := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	resp, err := c.Get(plain.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || string(body) != "普通 HTTP 正文" {
		t.Fatalf("body=%q err=%v", body, err)
	}
	secure := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "TLS 透明传输") }))
	secure.EnableHTTP2 = true
	secure.StartTLS()
	defer secure.Close()
	tlsTransport := p.Transport()
	defer tlsTransport.CloseIdleConnections()
	tlsTransport.TLSClientConfig = secure.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	c.Transport = tlsTransport
	resp, err = c.Get(secure.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || string(body) != "TLS 透明传输" || resp.ProtoMajor != 2 {
		t.Fatalf("body=%q proto=%s err=%v", body, resp.Proto, err)
	}
}

func TestE2ESSEStreamsBeforeCompletion(t *testing.T) {
	endpoint, _ := newTunnelServer(t, true)
	p := newTestProxy(t, endpoint, nil)
	release := make(chan struct{})
	finished := make(chan struct{})
	var once sync.Once

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, "data: second\n\n")
		w.(http.Flusher).Flush()
	}))
	defer func() { once.Do(func() { close(release) }); target.Close() }()
	tr := p.Transport()
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Get(target.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	first := make([]byte, len("data: first\n\n"))
	if _, err = io.ReadFull(resp.Body, first); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
		t.Fatal("目标结束后才收到第一条事件")
	default:
	}
	if string(first) != "data: first\n\n" {
		t.Fatalf("first=%q", first)
	}
	once.Do(func() { close(release) })
	rest, err := io.ReadAll(resp.Body)
	if err != nil || string(rest) != "data: second\n\n" {
		t.Fatalf("rest=%q err=%v", rest, err)
	}
}

func TestE2EWebSocketClients(t *testing.T) {
	endpoint, _ := newTunnelServer(t, true)
	p := newTestProxy(t, endpoint, nil)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		typ, b, err := c.Read(r.Context())
		if err == nil {
			c.Write(r.Context(), typ, b)
		}
	}))
	defer target.Close()
	u := "ws" + strings.TrimPrefix(target.URL, "http")
	t.Run("coder", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tr := p.Transport()
		defer tr.CloseIdleConnections()
		c, _, err := websocket.Dial(ctx, u, &websocket.DialOptions{HTTPClient: &http.Client{Transport: tr}})
		if err != nil {
			t.Fatal(err)
		}
		defer c.CloseNow()
		if err = c.Write(ctx, websocket.MessageText, []byte("coder echo")); err != nil {
			t.Fatal(err)
		}
		_, b, err := c.Read(ctx)
		if err != nil || string(b) != "coder echo" {
			t.Fatalf("echo=%q err=%v", b, err)
		}
	})
	t.Run("gorilla", func(t *testing.T) {
		d := gorilla.Dialer{NetDialContext: p.DialContext, HandshakeTimeout: 5 * time.Second}
		c, _, err := d.Dial(u, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err = c.WriteMessage(gorilla.BinaryMessage, []byte("gorilla echo")); err != nil {
			t.Fatal(err)
		}
		_, b, err := c.ReadMessage()
		if err != nil || string(b) != "gorilla echo" {
			t.Fatalf("echo=%q err=%v", b, err)
		}
	})
}

// 观察中继逐条转发 WS 消息，直接验证实际线上消息边界。
func messageObserver(t *testing.T, endpoint string, limit int) (string, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	var up, down atomic.Int64
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer a.CloseNow()
		a.SetReadLimit(256 * 1024)
		b, _, err := websocket.Dial(r.Context(), endpoint, nil)
		if err != nil {
			return
		}
		defer b.CloseNow()
		b.SetReadLimit(256 * 1024)
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		forward := func(dst, src *websocket.Conn, count *atomic.Int64) {
			defer cancel()
			for {
				typ, data, err := src.Read(ctx)
				if err != nil {
					return
				}
				if typ == websocket.MessageBinary {
					count.Add(1)
					if len(data) > limit {
						t.Errorf("二进制消息 %d 超过上限 %d", len(data), limit)
						return
					}
				}
				if dst.Write(ctx, typ, data) != nil {
					return
				}
			}
		}
		done := make(chan struct{})
		go func() { defer close(done); forward(b, a, &up) }()
		forward(a, b, &down)
		cancel()
		<-done
	}))
	t.Cleanup(s.Close)
	return "ws" + strings.TrimPrefix(s.URL, "http"), &up, &down
}

func tcpEcho(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	conns := map[net.Conn]bool{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns[c] = true
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				io.Copy(c, c)
				mu.Lock()
				delete(conns, c)
				mu.Unlock()
			}()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		for c := range conns {
			c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return ln.Addr().String()
}

func TestE2ELargeBidirectionalDataAndMessageBounds(t *testing.T) {
	target := tcpEcho(t)
	endpoint, _ := newTunnelServer(t, true)
	observed, up, down := messageObserver(t, endpoint, 8192)
	p := newTestProxy(t, observed, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := p.DialContext(ctx, "tcp", target)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(15 * time.Second))
	data := make([]byte, 2*1024*1024+137)
	if _, err = rand.Read(data); err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	go func() { _, err := io.Copy(c, bytes.NewReader(data)); written <- err }()
	got := make([]byte, len(data))
	if _, err = io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	if err = <-written; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, got) {
		t.Fatal("双向随机数据不一致")
	}
	if up.Load() < 257 || down.Load() < 257 {
		t.Fatalf("分片数 up=%d down=%d", up.Load(), down.Load())
	}
}

func TestE2EHandshakeErrorsDoNotRetry(t *testing.T) {
	endpoint, count := newTunnelServer(t, true)
	p := newTestProxy(t, endpoint, func(c *Config) {
		c.Token = "错误令牌"
		c.MaxRetries = 4
		c.Endpoints = []string{endpoint, endpoint}
	})
	_, err := p.DialContext(context.Background(), "tcp", "127.0.0.1:1234")
	var h *HandshakeError
	if !errors.As(err, &h) || h.Code != "auth_failed" {
		t.Fatalf("err=%v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("明确握手失败后仍重试: %d", count.Load())
	}
	endpoint, _ = newTunnelServer(t, false)
	p = newTestProxy(t, endpoint, nil)
	_, err = p.DialContext(context.Background(), "tcp", "127.0.0.1:1234")
	if !errors.As(err, &h) || h.Code != "forbidden_target" {
		t.Fatalf("err=%v", err)
	}
}

func TestE2EBypassDoesNotReachTunnel(t *testing.T) {
	target := tcpEcho(t)
	endpoint, count := newTunnelServer(t, true)
	for _, rule := range []string{"127.0.0.1", "127.0.0.0/8"} {
		t.Run(rule, func(t *testing.T) {
			p := newTestProxy(t, endpoint, func(c *Config) { c.Bypass = []string{rule} })
			c, err := p.DialContext(context.Background(), "tcp", target)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(3 * time.Second))
			c.Write([]byte("direct"))
			b := make([]byte, 6)
			if _, err = io.ReadFull(c, b); err != nil || string(b) != "direct" {
				t.Fatalf("body=%q err=%v", b, err)
			}
		})
	}
	if count.Load() != 0 {
		t.Fatalf("绕过地址仍经过服务端: %d", count.Load())
	}
}

func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("等待条件超时")
}

func TestE2ERetryAndCooldown(t *testing.T) {
	target := tcpEcho(t)
	good, _ := newTunnelServer(t, true)
	backendURL, err := url.Parse("http" + strings.TrimPrefix(good, "ws"))
	if err != nil {
		t.Fatal(err)
	}
	for _, strategy := range []Strategy{Random, StickyByHost} {
		t.Run(fmt.Sprint(strategy), func(t *testing.T) {
			// 首次选中的 endpoint 固定返回 503，另一 endpoint 可用，使 Random 测试不依赖随机结果。
			var mu sync.Mutex
			badPath := ""
			var failures atomic.Int64
			relay := httputil.NewSingleHostReverseProxy(backendURL)
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				if badPath == "" {
					badPath = r.URL.Path
				}
				bad := r.URL.Path == badPath
				mu.Unlock()
				if bad {
					failures.Add(1)
					http.Error(w, "不可用", http.StatusServiceUnavailable)
					return
				}
				relay.ServeHTTP(w, r)
			}))
			defer gateway.Close()
			endpoint := "ws" + strings.TrimPrefix(gateway.URL, "http")
			p := newTestProxy(t, endpoint, func(c *Config) {
				c.Endpoints = []string{endpoint + "/a", endpoint + "/b"}
				c.Strategy = strategy
				c.MaxRetries = 2
				c.EndpointCooldown = time.Minute
			})
			c, err := p.DialContext(context.Background(), "tcp", target)
			if err != nil {
				t.Fatal(err)
			}
			c.Close()
			p.mu.Lock()
			cooling := 0
			for _, until := range p.cooldown {
				if time.Now().Before(until) {
					cooling++
				}
			}
			p.mu.Unlock()
			if cooling != 1 || failures.Load() != 1 {
				t.Fatalf("未重试或未冷却 cooling=%d failures=%d", cooling, failures.Load())
			}
			for i := 0; i < 5; i++ {
				c, err := p.DialContext(context.Background(), "tcp", target)
				if err != nil {
					t.Fatal(err)
				}
				c.Close()
			}
			if failures.Load() != 1 {
				t.Fatal("冷却中的 endpoint 仍被访问")
			}
		})
	}
}

func TestE2EPrewarmReusedAndClose(t *testing.T) {
	target := tcpEcho(t)
	endpoint, count := newTunnelServer(t, true)
	p := newTestProxy(t, endpoint, func(c *Config) { c.Prewarm = 1; c.PrewarmMaxIdle = time.Minute; c.PingInterval = 20 * time.Millisecond })
	eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return len(p.pool) == 1 })
	c, err := p.DialContext(context.Background(), "tcp", target)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return len(p.pool) == 1 })
	c.Close()
	p.mu.Lock()
	warm := p.pool[0]
	p.mu.Unlock()
	before := count.Load()
	second, err := p.DialContext(context.Background(), "tcp", target)
	if err != nil {
		t.Fatal(err)
	}
	if second.(*tunnelConn).w != warm {
		t.Fatal("第二次拨号未复用预热连接")
	}
	// 取出池中的连接不等待新的 WS 建连；补池允许异步新建一条。
	if count.Load() > before+1 {
		t.Fatalf("第二次拨号重复建连 before=%d after=%d", before, count.Load())
	}
	second.SetDeadline(time.Now().Add(3 * time.Second))
	second.Write([]byte("warm"))
	b := make([]byte, 4)
	if _, err = io.ReadFull(second, b); err != nil || string(b) != "warm" {
		t.Fatalf("body=%q err=%v", b, err)
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = p.DialContext(context.Background(), "tcp", target); err == nil {
		t.Fatal("关闭后仍可拨号")
	}
	second.SetReadDeadline(time.Now().Add(time.Second))
	if _, err = second.Read(make([]byte, 1)); err == nil {
		t.Fatal("Close 未关闭活动连接")
	}
}
