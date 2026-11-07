package scfproxy_test

import (
	"context"
	"log"
	"net/http"
	"os"

	coder "github.com/coder/websocket"
	gorilla "github.com/gorilla/websocket"
	"github.com/zcw199604/scfproxy/pkg/scfpool"
	"github.com/zcw199604/scfproxy/pkg/scfproxy"
)

// 以下Example不含Output，仅编译，不会在测试中连接网络或云服务。
func Example() {
	ctx := context.Background()
	p, err := scfproxy.New(scfproxy.Config{
		EndpointsFile: "endpoints.txt",
		Token:         os.Getenv("SCFPROXY_TOKEN"),
	})
	if err != nil {
		log.Print(err)
		return
	}
	defer p.Close()
	if err := useClients(ctx, p); err != nil {
		log.Print(err)
	}
}

func ExampleProxy_poolFile() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	packageData, err := os.ReadFile("dist/scf-server.zip")
	if err != nil {
		log.Print(err)
		return
	}
	mgr, err := scfpool.NewManagerFromEnv(scfpool.Options{
		PoolFile: "pool.json",
		Regions:  []string{"ap-shanghai", "ap-beijing", "ap-chengdu"},
		Size:     2,
		Token:    os.Getenv("SCFPROXY_TOKEN"),
		Package:  packageData,
	})
	if err != nil {
		log.Print(err)
		return
	}
	// 空池才初始化创建；已有池保留其主用。
	if err := mgr.Init(ctx); err != nil {
		log.Print(err)
		return
	}
	p, err := scfproxy.New(scfproxy.Config{
		PoolFile: "pool.json",
		Token:    os.Getenv("SCFPROXY_TOKEN"),
		OnSwitch: func(old, next scfproxy.EndpointInfo, reason string) {
			log.Printf("出口切换 %s -> %s: %s", old.EgressIP, next.EgressIP, reason)
			// OnSwitch已由库异步调用，补位器内部串行，无需再创建协程。
			if _, err := mgr.Replenish(ctx); err != nil {
				log.Printf("自动补位失败: %v", err)
			}
		},
	})
	if err != nil {
		log.Print(err)
		return
	}
	defer p.Close()
	if err := useClients(ctx, p); err != nil {
		log.Print(err)
	}
	// 业务确认IP被封时显式调用p.Switch(reason)，临时故障不会自动切主用。
}

// useClients 对普通与固定IP模式使用完全相同的HTTP和WebSocket接入方式。
func useClients(ctx context.Context, p *scfproxy.Proxy) error {
	transport := p.Transport()
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Transport: transport}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.com/", nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	// SSE可以在此逐段读取resp.Body，无需等待完整响应。
	resp.Body.Close()

	gorillaDialer := gorilla.Dialer{NetDialContext: p.DialContext}
	g, _, err := gorillaDialer.DialContext(ctx, "wss://example.com/ws", nil)
	if err != nil {
		return err
	}
	defer g.Close()
	c, _, err := coder.Dial(ctx, "wss://example.com/ws", &coder.DialOptions{HTTPClient: httpClient})
	if err != nil {
		return err
	}
	defer c.Close(coder.StatusNormalClosure, "")
	return nil
}
