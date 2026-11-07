// Package server 实现 SCF WebSocket 到 TCP 的隧道服务。
package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/zcw199604/scfproxy/internal/protocol"
)

type Config struct {
	Token            string
	AllowPrivate     bool
	HandshakeTimeout time.Duration
	// PrewarmTimeout 是等待首条消息的上限；收到消息头后使用 HandshakeTimeout。
	PrewarmTimeout       time.Duration
	DialTimeout          time.Duration
	MaxMessageSize       int
	MaxMessagesPerSecond int
	MaxBytesPerSecond    int
	Logger               *slog.Logger
}

type Server struct {
	cfg    Config
	mu     sync.Mutex
	closed bool
	active map[*websocket.Conn]context.CancelFunc
	nonces map[string]time.Time
}

func New(cfg Config) (*Server, error) {
	if cfg.Token == "" {
		return nil, errors.New("SCFPROXY_TOKEN 不能为空")
	}
	if cfg.HandshakeTimeout == 0 {
		cfg.HandshakeTimeout = 10 * time.Second
	}
	if cfg.PrewarmTimeout == 0 {
		cfg.PrewarmTimeout = 5 * time.Minute
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	if cfg.MaxMessageSize == 0 {
		cfg.MaxMessageSize = 64 * 1024
	}
	if cfg.HandshakeTimeout < 0 || cfg.PrewarmTimeout < 0 || cfg.DialTimeout < 0 || cfg.MaxMessageSize < 1 || cfg.MaxMessageSize > 256*1024 || cfg.MaxMessagesPerSecond < 0 || cfg.MaxBytesPerSecond < 0 {
		return nil, errors.New("无效的服务端配置")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Server{cfg: cfg, active: make(map[*websocket.Conn]context.CancelFunc), nonces: make(map[string]time.Time)}, nil
}

func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	conns := make(map[*websocket.Conn]context.CancelFunc, len(s.active))
	for c, cancel := range s.active {
		conns[c] = cancel
	}
	s.mu.Unlock()
	var wg sync.WaitGroup
	for c, cancel := range conns {
		wg.Add(1)
		go func(c *websocket.Conn, cancel context.CancelFunc) {
			defer wg.Done()
			c.Close(websocket.StatusNormalClosure, "")
			cancel()
		}(c, cancel)
	}
	wg.Wait()
	return nil
}

func (s *Server) useNonce(r protocol.Request) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, expiry := range s.nonces {
		if !expiry.After(now) {
			delete(s.nonces, k)
		}
	}
	if _, ok := s.nonces[r.Nonce]; ok {
		return false
	}
	// 拒绝新请求而不是驱逐尚未过期的 nonce，保持缓存内防重放保证。
	if len(s.nonces) >= 65536 {
		return false
	}
	s.nonces[r.Nonce] = time.Unix(r.TS+301, 0)
	return true
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !websocketUpgrade(r) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
		return
	}
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true, CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		cancel()
		ws.Close(websocket.StatusNormalClosure, "")
		return
	}
	s.active[ws] = cancel
	s.mu.Unlock()
	defer func() {
		cancel()
		ws.Close(websocket.StatusNormalClosure, "")
		s.mu.Lock()
		delete(s.active, ws)
		s.mu.Unlock()
	}()
	s.cfg.Logger.Info("连接建立", "remote", r.RemoteAddr)
	var target, signature string
	var up, down int64
	var reason = "握手结束"
	defer func() {
		s.cfg.Logger.Info("连接结束", "target", s.diagnostic(target, signature), "reason", s.diagnostic(reason, signature), "up_bytes", up, "down_bytes", down)
	}()
	ws.SetReadLimit(4096)
	preCtx, preCancel := context.WithTimeout(ctx, s.cfg.PrewarmTimeout)
	typ, reader, err := ws.Reader(preCtx)
	if err != nil {
		preCancel()
		return
	}
	timer := time.AfterFunc(s.cfg.HandshakeTimeout, preCancel)
	data, err := io.ReadAll(reader)
	timer.Stop()
	preCancel()
	if err != nil {
		return
	}
	respond := func(resp protocol.Response) bool {
		c, stop := context.WithTimeout(ctx, s.cfg.HandshakeTimeout)
		defer stop()
		b, _ := json.Marshal(resp)
		return ws.Write(c, websocket.MessageText, b) == nil
	}
	fail := func(code, msg string) { respond(protocol.Response{Code: code, Error: msg}) }
	var req protocol.Request
	if typ != websocket.MessageText || json.Unmarshal(data, &req) != nil {
		fail("bad_request", "握手必须是 JSON 文本")
		return
	}
	if !protocol.Verify(s.cfg.Token, req, time.Now()) || !s.useNonce(req) {
		fail("auth_failed", "握手认证失败")
		return
	}
	target = req.Target
	signature = req.Sign
	dialCtx, dialCancel := context.WithTimeout(ctx, s.cfg.DialTimeout)
	tcp, err := s.dialTarget(dialCtx, target)
	dialCancel()
	if err != nil {
		code := "dial_failed"
		if errors.Is(err, errForbidden) {
			code = "forbidden_target"
		}
		if errors.Is(err, errBadTarget) {
			code = "bad_request"
		}
		reason = s.diagnostic(err.Error(), signature)
		s.cfg.Logger.Warn("目标连接失败", "target", s.diagnostic(target, signature), "code", code, "error", reason)
		fail(code, reason)
		return
	}
	defer tcp.Close()
	s.cfg.Logger.Info("目标已连接", "target", s.diagnostic(target, signature))
	if !respond(protocol.Response{OK: true}) {
		return
	}
	ws.SetReadLimit(256 * 1024)
	type result struct {
		bytes int64
		err   error
	}
	uploads := make(chan result, 1)
	go func() {
		var total int64
		for {
			typ, data, err := ws.Read(ctx)
			if err != nil {
				uploads <- result{total, err}
				return
			}
			if typ != websocket.MessageBinary {
				uploads <- result{total, errors.New("隧道仅允许二进制消息")}
				return
			}
			n, err := tcp.Write(data)
			total += int64(n)
			if err != nil {
				uploads <- result{total, err}
				return
			}
		}
	}()
	downloads := make(chan result, 1)
	go func() {
		var total int64
		buf := make([]byte, s.cfg.MaxMessageSize)
		var next time.Time
		for {
			n, err := tcp.Read(buf)
			if n > 0 {
				if s.cfg.MaxMessagesPerSecond > 0 || s.cfg.MaxBytesPerSecond > 0 {
					if delay := time.Until(next); delay > 0 {
						timer := time.NewTimer(delay)
						select {
						case <-ctx.Done():
							timer.Stop()
							downloads <- result{total, ctx.Err()}
							return
						case <-timer.C:
						}
					}
					interval := time.Duration(0)
					if s.cfg.MaxMessagesPerSecond > 0 {
						interval = time.Second / time.Duration(s.cfg.MaxMessagesPerSecond)
					}
					if s.cfg.MaxBytesPerSecond > 0 {
						byteInterval := time.Duration(n) * time.Second / time.Duration(s.cfg.MaxBytesPerSecond)
						if byteInterval > interval {
							interval = byteInterval
						}
					}
					next = time.Now().Add(interval)
				}
				if writeErr := ws.Write(ctx, websocket.MessageBinary, buf[:n]); writeErr != nil {
					downloads <- result{total, writeErr}
					return
				}
				total += int64(n)
			}
			if err != nil {
				downloads <- result{total, err}
				return
			}
		}
	}()
	select {
	case res := <-uploads:
		up = res.bytes
		reason = res.err.Error()
		tcp.Close()
		ws.Close(websocket.StatusNormalClosure, "")
		cancel()
		down = (<-downloads).bytes
	case res := <-downloads:
		down = res.bytes
		reason = res.err.Error()
		tcp.Close()
		ws.Close(websocket.StatusNormalClosure, "")
		cancel()
		up = (<-uploads).bytes
	}
}

func websocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}
