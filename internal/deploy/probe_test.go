package deploy

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zcw199604/scfproxy/internal/server"
	"github.com/zcw199604/scfproxy/pkg/scfproxy"
)

func localProbeEndpoint(t *testing.T) string {
	t.Helper()
	h, err := server.New(server.Config{Token: "local-probe-token", AllowPrivate: true})
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(h)
	t.Cleanup(func() { h.Close(); s.Close() })
	return "ws" + strings.TrimPrefix(s.URL, "http")
}

func TestCheckUsesIndividualTunnelAndLimitsResponse(t *testing.T) {
	var requests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); w.Write(bytes.Repeat([]byte("x"), 512)) }))
	defer target.Close()
	endpoint := localProbeEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := Check(ctx, CheckOptions{Endpoints: []string{endpoint, endpoint}, Token: "local-probe-token", Target: target.URL, Concurrency: 2})
	if len(results) != 2 {
		t.Fatalf("results=%v", results)
	}
	for _, r := range results {
		if r.Err != nil || r.Status != 200 || r.Body != strings.Repeat("x", 100) || r.DialDuration <= 0 || r.FirstByteDuration < r.DialDuration {
			t.Fatalf("result=%+v", r)
		}
	}
	if requests.Load() != 2 {
		t.Fatalf("requests=%d", requests.Load())
	}
}

func TestBenchDownloadsUploadsAndHoldsSameConnection(t *testing.T) {
	var uploaded atomic.Int64
	var connections atomic.Int64
	target := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			n, err := io.Copy(io.Discard, r.Body)
			if err != nil {
				t.Error(err)
			}
			uploaded.Add(n)
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Write(bytes.Repeat([]byte("d"), 4096))
	}))
	target.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	target.Start()
	defer target.Close()
	endpoint := localProbeEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := Bench(ctx, BenchOptions{Endpoints: []string{endpoint}, Token: "local-probe-token", DownloadURL: target.URL, UploadURL: target.URL, Bytes: 4096, NoRateLimit: true, Hold: 30 * time.Millisecond})
	if len(results) != 3 {
		t.Fatalf("results=%v", results)
	}
	for _, r := range results {
		if r.Err != nil || r.Duration <= 0 {
			t.Fatalf("result=%+v", r)
		}
		if r.Operation != "hold" && r.Bytes != 4096 {
			t.Fatalf("result=%+v", r)
		}
		if r.Operation == "hold" && r.Duration < 30*time.Millisecond {
			t.Fatalf("未实际保持空闲: %+v", r)
		}
	}
	if uploaded.Load() != 4096 {
		t.Fatalf("uploaded=%d", uploaded.Load())
	}
	// 下载、上传和保持测试各建一条连接；保持后的请求不得重新拨号。
	if connections.Load() != 3 {
		t.Fatalf("hold 未复用同一 TCP 连接: connections=%d", connections.Load())
	}
}

func TestProbeRejectsUntrustedHTTPS(t *testing.T) {
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("不应绕过证书校验")) }))
	defer target.Close()
	endpoint := localProbeEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := Check(ctx, CheckOptions{Endpoints: []string{endpoint}, Token: "local-probe-token", Target: target.URL, Concurrency: 1})
	if len(results) != 1 || results[0].Err == nil {
		t.Fatalf("必须验证 TLS 证书: %+v", results)
	}
	results2 := Bench(ctx, BenchOptions{Endpoints: []string{endpoint}, Token: "local-probe-token", DownloadURL: target.URL, UploadURL: target.URL, Bytes: 10, Hold: time.Millisecond, NoRateLimit: true})
	for _, r := range results2 {
		if r.Err == nil {
			t.Fatalf("必须验证 TLS 证书: %+v", r)
		}
	}
}

func TestParseBytes(t *testing.T) {
	for _, tt := range []struct {
		input string
		want  int64
	}{{"10MB", 10000000}, {"1MiB", 1048576}, {"128KB", 128000}, {"4096", 4096}, {"2gb", 2000000000}} {
		got, err := ParseBytes(tt.input)
		if err != nil || got != tt.want {
			t.Fatalf("%s: got=%d err=%v", tt.input, got, err)
		}
	}
	for _, input := range []string{"", "0", "-1", "bogus", "999999999999999999999GB", "1.5MB"} {
		if _, err := ParseBytes(input); err == nil {
			t.Fatalf("应拒绝 %q", input)
		}
	}
}

func TestBenchUploadRejectsEarlySuccess(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
	}))
	defer target.Close()
	endpoint := localProbeEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := Bench(ctx, BenchOptions{Endpoints: []string{endpoint}, Token: "local-probe-token", DownloadURL: target.URL, UploadURL: target.URL, Bytes: 16 * 1024 * 1024, NoRateLimit: true})
	result := results[1]
	if result.Err == nil || !strings.Contains(result.Err.Error(), "上传不完整") || result.Bytes >= 16*1024*1024 {
		t.Fatalf("提前响应不能认定上传完整: %+v", result)
	}
}

func TestBenchDownloadCountsCompressedBytes(t *testing.T) {
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	zw.Write(bytes.Repeat([]byte("compressible"), 1024))
	zw.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept-Encoding") != "" {
			t.Error("测速不应自动请求压缩响应")
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Write(compressed.Bytes())
	}))
	defer target.Close()
	endpoint := localProbeEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := scfproxy.New(scfproxy.Config{Endpoints: []string{endpoint}, Token: "local-probe-token", DisableRateLimit: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	n, warning, err := benchTransfer(ctx, p, BenchOptions{DownloadURL: target.URL, Bytes: int64(compressed.Len())}, "download")
	if err != nil || warning != nil || n != int64(compressed.Len()) {
		t.Fatalf("应统计实际压缩响应字节: n=%d want=%d err=%v", n, compressed.Len(), err)
	}
}

func TestBenchNonSuccessPreservesTransferredBytes(t *testing.T) {
	var uploaded atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			n, _ := io.Copy(io.Discard, r.Body)
			uploaded.Store(n)
		}
		w.WriteHeader(http.StatusBadRequest)
		w.Write(bytes.Repeat([]byte("x"), 512))
	}))
	defer target.Close()
	results := Bench(context.Background(), BenchOptions{Endpoints: []string{localProbeEndpoint(t)}, Token: "local-probe-token", DownloadURL: target.URL, UploadURL: target.URL, Bytes: 1024, NoRateLimit: true})
	if results[0].Bytes != 512 || results[1].Bytes != 1024 || uploaded.Load() != 1024 {
		t.Fatalf("非2xx丢失已传输字节: %+v", results)
	}
	for _, r := range results {
		if r.Err != nil || r.Warning == nil || !strings.Contains(r.Warning.Error(), "400") {
			t.Fatalf("状态错误应保留为警告: %+v", r)
		}
	}
}

func TestBenchDownloadLimitClosesBodyAndSkipsUnspecifiedUpload(t *testing.T) {
	closed := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(closed)
		for {
			if _, err := w.Write(bytes.Repeat([]byte("x"), 4096)); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
				return
			default:
			}
		}
	}))
	defer target.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results := Bench(ctx, BenchOptions{Endpoints: []string{localProbeEndpoint(t)}, Token: "local-probe-token", DownloadURL: target.URL, Bytes: 1024, NoRateLimit: true})
	if len(results) != 1 || results[0].Operation != "download" || results[0].Bytes != 1024 || results[0].Err != nil {
		t.Fatalf("下载应限量且跳过未指定上传: %+v", results)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("达到下载限制后响应体未关闭")
	}
}
