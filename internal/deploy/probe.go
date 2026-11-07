package deploy

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zcw199604/scfproxy/pkg/scfproxy"
)

type CheckOptions struct {
	Endpoints   []string
	Token       string
	Target      string
	Concurrency int
}

type CheckResult struct {
	Endpoint     string
	DialDuration time.Duration
	// FirstByteDuration 从开始请求计时，包含隧道、TLS 和目标响应等待。
	FirstByteDuration time.Duration
	Status            int
	Body              string
	Err               error
}

// Check 为每个出口单独创建代理，不通过出口策略跳过失败节点。
func Check(ctx context.Context, opts CheckOptions) []CheckResult {
	if opts.Target == "" {
		opts.Target = "https://api.ipify.org"
	}
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = 4
	}
	results := make([]CheckResult, len(opts.Endpoints))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for i := 0; i < min(concurrency, len(results)); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				results[index] = checkEndpoint(ctx, opts, opts.Endpoints[index])
			}
		}()
	}
	for i := range results {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return results
}

func checkEndpoint(ctx context.Context, opts CheckOptions, endpoint string) CheckResult {
	result := CheckResult{Endpoint: endpoint}
	p, err := scfproxy.New(scfproxy.Config{Endpoints: []string{endpoint}, Token: opts.Token})
	if err != nil {
		result.Err = probeError(err, opts.Token)
		return result
	}
	defer p.Close()
	tr := p.Transport()
	tr.DisableKeepAlives = true
	defer tr.CloseIdleConnections()
	var mu sync.Mutex
	originalDial := tr.DialContext
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		start := time.Now()
		conn, err := originalDial(ctx, network, addr)
		mu.Lock()
		result.DialDuration = time.Since(start)
		mu.Unlock()
		return conn, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	start := time.Now()
	trace := &httptrace.ClientTrace{GotFirstResponseByte: func() { mu.Lock(); result.FirstByteDuration = time.Since(start); mu.Unlock() }}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodGet, opts.Target, nil)
	if err == nil {
		client := &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		var resp *http.Response
		resp, err = client.Do(req)
		if err == nil {
			result.Status = resp.StatusCode
			var body []byte
			body, err = io.ReadAll(io.LimitReader(resp.Body, 100))
			resp.Body.Close()
			result.Body = probeRedact(string(body), opts.Token)
		}
	}
	// DialContext 和 trace 可能运行在 Transport 的独立协程中。
	mu.Lock()
	result.Err = probeError(err, opts.Token)
	snapshot := result
	mu.Unlock()
	return snapshot
}

type BenchOptions struct {
	Endpoints   []string
	Token       string
	DownloadURL string
	UploadURL   string
	Bytes       int64
	NoRateLimit bool
	Hold        time.Duration
}

type BenchResult struct {
	Operation string
	Warning   error
	Bytes     int64
	Duration  time.Duration
	Err       error
}

// Bench 分别建立下载、上传与可选空闲测试请求，返回每一项结果。
func Bench(ctx context.Context, opts BenchOptions) []BenchResult {
	if opts.Bytes == 0 {
		opts.Bytes = 10 * 1000 * 1000
	}
	if opts.DownloadURL == "" {
		opts.DownloadURL = "https://mirrors.aliyun.com/ubuntu/ls-lR.gz"
	}
	operations := []string{"download"}
	if opts.UploadURL != "" {
		operations = append(operations, "upload")
	}
	if opts.Hold > 0 {
		operations = append(operations, "hold")
	}
	results := make([]BenchResult, 0, len(operations))
	p, proxyErr := scfproxy.New(scfproxy.Config{Endpoints: opts.Endpoints, Token: opts.Token, DisableRateLimit: opts.NoRateLimit})
	if p != nil {
		defer p.Close()
	}
	for _, operation := range operations {
		result := BenchResult{Operation: operation}
		start := time.Now()
		if opts.Bytes <= 0 || opts.Hold < 0 {
			result.Err = errors.New("bytes 必须大于 0，hold 不能为负数")
		} else if proxyErr != nil {
			result.Err = proxyErr
		} else {
			operationCtx, cancel := context.WithTimeout(ctx, opts.Hold+5*time.Minute)
			if operation == "hold" {
				result.Bytes, result.Warning, result.Err = benchHold(operationCtx, p, opts)
			} else {
				result.Bytes, result.Warning, result.Err = benchTransfer(operationCtx, p, opts, operation)
			}
			cancel()
		}
		result.Duration = time.Since(start)
		result.Err = probeError(result.Err, opts.Token)
		result.Warning = probeError(result.Warning, opts.Token)
		results = append(results, result)
	}
	return results
}

func benchTransfer(ctx context.Context, p *scfproxy.Proxy, opts BenchOptions, operation string) (int64, error, error) {
	tr := p.Transport()
	tr.DisableKeepAlives = true
	tr.DisableCompression = true
	defer tr.CloseIdleConnections()
	method, target := http.MethodGet, opts.DownloadURL
	var body io.Reader
	var uploaded probeCountingReader
	if operation == "upload" {
		method, target = http.MethodPost, opts.UploadURL
		uploaded.reader = io.LimitReader(probeZeroReader{}, opts.Bytes)
		body = &uploaded
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return 0, nil, err
	}
	if operation == "upload" {
		req.ContentLength = opts.Bytes
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	client := &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return uploaded.count.Load(), nil, err
	}
	defer resp.Body.Close()
	warning := benchStatusWarning(resp.StatusCode)
	if operation == "download" {
		n, err := io.Copy(io.Discard, io.LimitReader(resp.Body, opts.Bytes))
		return n, warning, err
	}
	// 上传响应体不计入吞吐；请求体计数保留，包括非2xx和不完整上传。
	_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))
	n := uploaded.count.Load()
	if n < opts.Bytes {
		err = errors.Join(err, fmt.Errorf("上传不完整：请求体实际读取 %d / %d 字节", n, opts.Bytes))
	}
	return n, warning, err
}

func benchStatusWarning(status int) error {
	if status < 200 || status >= 300 {
		return fmt.Errorf("HTTP 状态 %d", status)
	}
	return nil
}

func benchHold(ctx context.Context, p *scfproxy.Proxy, opts BenchOptions) (int64, error, error) {
	u, err := url.Parse(opts.DownloadURL)
	if err != nil {
		return 0, nil, err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return 0, nil, errors.New("hold 目标必须是完整的 HTTP/HTTPS URL")
	}
	port := u.Port()
	if port == "" {
		port = "80"
		if u.Scheme == "https" {
			port = "443"
		}
	}
	conn, err := p.DialContext(ctx, "tcp", net.JoinHostPort(u.Hostname(), port))
	if err != nil {
		return 0, nil, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	}
	timer := time.NewTimer(opts.Hold)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
	// 保持阶段不发送任何 TCP 数据；TLS 也在空闲结束后通过同一隧道协商。
	var stream net.Conn = conn
	if u.Scheme == "https" {
		secure := tls.Client(conn, &tls.Config{ServerName: u.Hostname(), MinVersion: tls.VersionTLS12})
		if err := secure.HandshakeContext(ctx); err != nil {
			return 0, nil, err
		}
		stream = secure
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, opts.DownloadURL, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Close = true
	if err = req.Write(stream); err != nil {
		return 0, nil, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(stream), req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	n, err := io.Copy(io.Discard, io.LimitReader(resp.Body, opts.Bytes))
	return n, benchStatusWarning(resp.StatusCode), err
}

type probeCountingReader struct {
	reader io.Reader
	count  atomic.Int64
}

func (r *probeCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.count.Add(int64(n))
	return n, err
}

type probeZeroReader struct{}

func (probeZeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

// ParseBytes 接受十进制 KB/MB/GB/TB 及二进制 KiB/MiB/GiB/TiB。
func ParseBytes(input string) (int64, error) {
	s := strings.ToUpper(strings.TrimSpace(input))
	end := 0
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	n, err := strconv.ParseUint(s[:end], 10, 64)
	if err != nil || n == 0 {
		return 0, errors.New("bytes 必须为正整数，可带 KB/MB/GB 或 KiB/MiB/GiB 后缀")
	}
	multipliers := map[string]uint64{"": 1, "B": 1, "KB": 1000, "MB": 1000000, "GB": 1000000000, "TB": 1000000000000, "KIB": 1 << 10, "MIB": 1 << 20, "GIB": 1 << 30, "TIB": 1 << 40}
	factor, ok := multipliers[strings.TrimSpace(s[end:])]
	if !ok || n > math.MaxInt64/factor {
		return 0, errors.New("bytes 后缀无效或数值过大")
	}
	return int64(n * factor), nil
}

func probeRedact(value, token string) string {
	if token != "" {
		return strings.ReplaceAll(value, token, "[REDACTED]")
	}
	return value
}
func probeError(err error, token string) error {
	if err == nil {
		return nil
	}
	return errors.New(probeRedact(err.Error(), token))
}
