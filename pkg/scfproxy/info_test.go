package scfproxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
)

func TestTunnelInfoAndLegacyServer(t *testing.T) {
	endpoint, _ := newTunnelServer(t, true)
	legacy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		if _, _, err = ws.Read(r.Context()); err != nil {
			return
		}
		if err = ws.Write(r.Context(), websocket.MessageText, []byte(`{"ok":true}`)); err != nil {
			return
		}
		ws.Read(r.Context())
	}))
	defer legacy.Close()
	target := tcpEcho(t)
	for _, tc := range []struct {
		name, endpoint string
		legacy         bool
	}{{"current", endpoint, false}, {"legacy", "ws" + strings.TrimPrefix(legacy.URL, "http"), true}} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(Config{Endpoints: []string{tc.endpoint}, Token: testToken})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			c, err := p.DialContext(context.Background(), "tcp", target)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			info, ok := c.(TunnelInfo)
			if !ok {
				t.Fatal("连接未实现TunnelInfo")
			}
			if tc.legacy {
				if info.InstanceID() != "" || info.InstanceActive() != 0 {
					t.Fatal("旧服务端应返回空实例信息")
				}
			} else if len(info.InstanceID()) != 16 || info.InstanceActive() != 1 {
				t.Fatalf("实例信息: %q %d", info.InstanceID(), info.InstanceActive())
			}
		})
	}
}
