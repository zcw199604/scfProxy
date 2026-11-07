package server

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/zcw199604/scfproxy/internal/protocol"
)

func TestHandshakeInstanceAndActiveTunnels(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer target.Close()
	s, err := New(Config{Token: "secret", AllowPrivate: true})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s)
	defer ts.Close()
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var clients []*websocket.Conn
	defer func() {
		for _, ws := range clients {
			ws.Close(websocket.StatusNormalClosure, "")
		}
	}()
	var instance string
	connect := func(want int) *websocket.Conn {
		t.Helper()
		ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ws.CloseNow() })
		clients = append(clients, ws)
		req, _ := protocol.NewRequest("secret", strings.TrimPrefix(target.URL, "http://"))
		b, _ := json.Marshal(req)
		if err = ws.Write(ctx, websocket.MessageText, b); err != nil {
			t.Fatal(err)
		}
		_, b, err = ws.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var r struct {
			OK       bool   `json:"ok"`
			Instance string `json:"instance"`
			Active   int    `json:"active"`
		}
		if err = json.Unmarshal(b, &r); err != nil {
			t.Fatal(err)
		}
		decoded, e := hex.DecodeString(r.Instance)
		if !r.OK || e != nil || len(decoded) != 8 || r.Active != want {
			t.Fatalf("握手信息=%s want active=%d", b, want)
		}
		if instance != "" && r.Instance != instance {
			t.Fatal("同实例的ID不同")
		}
		instance = r.Instance
		go ws.Read(ctx)
		return ws
	}
	first := connect(1)
	connect(2)
	connect(3)
	first.Close(websocket.StatusNormalClosure, "")
	for {
		s.mu.Lock()
		n := len(s.active)
		s.mu.Unlock()
		if n == 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("关闭后连接数未减少")
		case <-time.After(time.Millisecond):
		}
	}
	connect(3)
}
