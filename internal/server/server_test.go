package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/zcw199604/scfproxy/internal/protocol"
)

func TestHandshakeAndReplay(t *testing.T) {
	s, err := New(Config{Token: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s)
	defer ts.Close()
	defer s.Close()
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "ok" {
		t.Fatal("健康检查失败")
	}
	req, err := protocol.NewRequest("secret", "127.0.0.1:443")
	if err != nil {
		t.Fatal(err)
	}
	for _, code := range []string{"forbidden_target", "auth_failed"} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		b, _ := json.Marshal(req)
		if err := ws.Write(ctx, websocket.MessageText, b); err != nil {
			t.Fatal(err)
		}
		typ, b, err := ws.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var response protocol.Response
		if typ != websocket.MessageText || json.Unmarshal(b, &response) != nil || response.OK || response.Code != code {
			t.Fatalf("握手回复 %s", b)
		}
		ws.Close(websocket.StatusNormalClosure, "")
		cancel()
	}
}

func TestPrewarmAndClose(t *testing.T) {
	s, _ := New(Config{Token: "secret", HandshakeTimeout: 20 * time.Millisecond, PrewarmTimeout: time.Second})
	ts := httptest.NewServer(s)
	defer ts.Close()
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()
	// 预热等待应超过握手消息读取时间，并持续处理 ping/pong。
	time.Sleep(50 * time.Millisecond)
	readDone := make(chan error, 1)
	go func() { _, _, err := ws.Read(ctx); readDone <- err }()
	if err := ws.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("服务关闭未终止连接")
		}
	case <-ctx.Done():
		t.Fatal("关闭连接超时")
	}
}

func TestRejectEmptyToken(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("空 token 必须拒绝")
	}
}

func TestDownstreamByteRate(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		c, err := listener.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		c.Write(bytes.Repeat([]byte("x"), 256))
	}()
	s, err := New(Config{Token: "secret", AllowPrivate: true, MaxMessageSize: 64, MaxBytesPerSecond: 512})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s)
	defer ts.Close()
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()
	req, _ := protocol.NewRequest("secret", listener.Addr().String())
	b, _ := json.Marshal(req)
	if err := ws.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ws.Read(ctx); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	total := 0
	for total < 256 {
		typ, data, err := ws.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if typ != websocket.MessageBinary || len(data) > 64 {
			t.Fatal("无效分片")
		}
		total += len(data)
	}
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Fatalf("字节限速未生效: %v", elapsed)
	}
}
