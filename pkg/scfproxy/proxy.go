// Package scfproxy 通过云函数 WebSocket 提供 TCP 出口隧道。
package scfproxy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/zcw199604/scfproxy/internal/protocol"
)

type Strategy int

const (
	Random Strategy = iota
	StickyByHost
	Pinned
)

// Config 配置隧道。负值 PingInterval 关闭保活；DisableRateLimit 关闭默认上行限速。
type Config struct {
	PoolFile             string
	BanTTL               time.Duration
	ReloadInterval       time.Duration
	OnSwitch             func(old, new EndpointInfo, reason string)
	Endpoints            []string
	EndpointsFile        string
	Token                string
	Strategy             Strategy
	Bypass               []string
	DialTimeout          time.Duration
	MaxRetries           int
	EndpointCooldown     time.Duration
	MaxMessageSize       int
	MaxMessagesPerSecond int
	MaxBytesPerSecond    int
	DisableRateLimit     bool
	CoalesceDelay        time.Duration
	PingInterval         time.Duration
	Prewarm              int
	PrewarmMaxIdle       time.Duration
	Logger               *slog.Logger
}

// HandshakeError 表示服务端明确拒绝，本次拨号不会重试。
type HandshakeError struct{ Code, Message string }

func (e *HandshakeError) Error() string { return fmt.Sprintf("scfproxy: %s: %s", e.Code, e.Message) }

type frame struct {
	typ  websocket.MessageType
	data []byte
	err  error
}
type warmConn struct {
	ws       *websocket.Conn
	endpoint int
	born     time.Time
	cancel   context.CancelFunc
	frames   chan frame
	done     chan struct{}
	abort    chan struct{}
	shutdown sync.Once
	idleDone chan struct{}
}
type Proxy struct {
	pinned    *pinnedState
	cfg       Config
	endpoints []string
	mu        sync.Mutex
	cooldown  []time.Time
	pool      []*warmConn
	active    map[*tunnelConn]struct{}
	closed    bool
	ctx       context.Context
	cancel    context.CancelFunc
	wake      chan struct{}
	wg        sync.WaitGroup
}

func New(cfg Config) (*Proxy, error) {
	if cfg.PoolFile != "" {
		return newPinnedProxy(cfg)
	}
	if cfg.Token == "" {
		return nil, errors.New("scfproxy: Token 不能为空")
	}
	endpoints := append([]string(nil), cfg.Endpoints...)
	if cfg.EndpointsFile != "" {
		f, e := os.Open(cfg.EndpointsFile)
		if e != nil {
			return nil, e
		}
		defer f.Close()
		s := bufio.NewScanner(f)
		for s.Scan() {
			line := strings.TrimSpace(s.Text())
			if line != "" && !strings.HasPrefix(line, "#") {
				endpoints = append(endpoints, line)
			}
		}
		if e = s.Err(); e != nil {
			return nil, e
		}
	}
	for i, e := range endpoints {
		e = strings.TrimSpace(e)
		u, err := url.Parse(e)
		if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil || u.Fragment != "" {
			return nil, fmt.Errorf("scfproxy: 无效 endpoint %q", e)
		}
		endpoints[i] = e
	}
	if len(endpoints) == 0 {
		return nil, errors.New("scfproxy: 至少需要一个 endpoint")
	}
	if cfg.Strategy == Pinned && len(endpoints) != 1 {
		return nil, errors.New("scfproxy: Pinned 必须只有一个 endpoint 或使用 PoolFile")
	}
	if cfg.Strategy != Random && cfg.Strategy != StickyByHost && cfg.Strategy != Pinned {
		return nil, errors.New("scfproxy: 无效策略")
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 15 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 2
	}
	if cfg.EndpointCooldown == 0 {
		cfg.EndpointCooldown = 30 * time.Second
	}
	if cfg.MaxMessageSize == 0 {
		cfg.MaxMessageSize = 64 * 1024
	}
	if cfg.MaxMessagesPerSecond == 0 && !cfg.DisableRateLimit {
		cfg.MaxMessagesPerSecond = 10
	}
	if cfg.MaxBytesPerSecond == 0 && !cfg.DisableRateLimit {
		cfg.MaxBytesPerSecond = 128 * 1024
	}
	if cfg.DisableRateLimit {
		cfg.MaxMessagesPerSecond = 0
		cfg.MaxBytesPerSecond = 0
	}
	if cfg.CoalesceDelay == 0 {
		cfg.CoalesceDelay = 2 * time.Millisecond
	}
	if cfg.PingInterval == 0 {
		cfg.PingInterval = 30 * time.Second
	}
	if cfg.PrewarmMaxIdle == 0 {
		cfg.PrewarmMaxIdle = 4 * time.Minute
	}
	if cfg.MaxMessageSize < 1 || cfg.MaxMessageSize > 256*1024 || cfg.DialTimeout < 0 || cfg.EndpointCooldown < 0 || cfg.CoalesceDelay < 0 || cfg.MaxMessagesPerSecond < 0 || cfg.MaxBytesPerSecond < 0 || cfg.Prewarm < 0 || cfg.PrewarmMaxIdle < 0 || cfg.MaxRetries < 0 {
		return nil, errors.New("scfproxy: 配置数值超出范围")
	}
	for _, b := range cfg.Bypass {
		if strings.Contains(b, "/") {
			if _, _, err := net.ParseCIDR(b); err != nil {
				return nil, fmt.Errorf("scfproxy: 无效 bypass CIDR %q", b)
			}
		}
	}
	cfg.Bypass = append([]string(nil), cfg.Bypass...)
	ctx, cancel := context.WithCancel(context.Background())
	p := &Proxy{cfg: cfg, endpoints: endpoints, cooldown: make([]time.Time, len(endpoints)), active: make(map[*tunnelConn]struct{}), ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1)}
	if cfg.Prewarm > 0 {
		p.wg.Add(1)
		go p.maintainPool()
	}
	return p, nil
}
func (p *Proxy) bypass(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	ip := net.ParseIP(host)
	for _, b := range p.cfg.Bypass {
		b = strings.ToLower(strings.TrimSuffix(b, "."))
		if _, n, e := net.ParseCIDR(b); e == nil {
			if ip != nil && n.Contains(ip) {
				return true
			}
			continue
		}
		if strings.HasPrefix(b, "*.") {
			if strings.HasSuffix(host, b[1:]) && len(host) > len(b)-1 {
				return true
			}
		} else if host == b || (ip != nil && ip.Equal(net.ParseIP(b))) {
			return true
		}
	}
	return false
}
func (p *Proxy) order(host string) []int {
	if p.cfg.Strategy == Pinned {
		order := make([]int, p.cfg.MaxRetries+1)
		return order
	}
	start := rand.IntN(len(p.endpoints))
	if p.cfg.Strategy == StickyByHost {
		h := fnv.New32a()
		_, _ = h.Write([]byte(strings.ToLower(host)))
		start = int(h.Sum32() % uint32(len(p.endpoints)))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// Random 优先复用池中任意健康出口；Sticky 必须保持 host 的映射。
	if p.cfg.Strategy == Random && len(p.pool) > 0 {
		offset := rand.IntN(len(p.pool))
		for n := range p.pool {
			w := p.pool[(offset+n)%len(p.pool)]
			if !time.Now().Before(p.cooldown[w.endpoint]) {
				start = w.endpoint
				break
			}
		}
	}
	out := make([]int, 0, len(p.endpoints))
	cooling := make([]int, 0, len(p.endpoints))
	now := time.Now()
	for n := range p.endpoints {
		i := (start + n) % len(p.endpoints)
		if !now.Before(p.cooldown[i]) {
			out = append(out, i)
		} else {
			cooling = append(cooling, i)
		}
	}
	// 冷却只降低优先级，不能阻止所有出口故障后的即时恢复探测。
	sort.SliceStable(cooling, func(a, b int) bool { return p.cooldown[cooling[a]].Before(p.cooldown[cooling[b]]) })
	return append(out, cooling...)
}
func (p *Proxy) connect(ctx context.Context, i int) (*warmConn, error) {
	ws, resp, err := websocket.Dial(ctx, p.endpoints[i], &websocket.DialOptions{HTTPClient: &http.Client{Transport: directTransport()}})
	if resp != nil && err != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	ws.SetReadLimit(256 * 1024)
	w := &warmConn{ws: ws, endpoint: i, born: time.Now(), frames: make(chan frame, 1), done: make(chan struct{}), abort: make(chan struct{})}
	stop := context.AfterFunc(p.ctx, func() { ws.CloseNow() })
	go func() {
		defer close(w.done)
		defer close(w.frames)
		defer stop()
		for {
			typ, b, e := ws.Read(p.ctx)
			select {
			case w.frames <- frame{typ, b, e}:
			case <-w.abort:
				return
			case <-p.ctx.Done():
				return
			}
			if e != nil {
				return
			}
		}
	}()
	return w, nil
}
func directTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	return t
}
func (w *warmConn) close() {
	w.shutdown.Do(func() { close(w.abort); w.ws.Close(websocket.StatusNormalClosure, "") })
}
func (w *warmConn) abortNow() { w.shutdown.Do(func() { close(w.abort); w.ws.CloseNow() }) }
func (p *Proxy) take(ctx context.Context, i int) *warmConn {
	p.mu.Lock()
	var found *warmConn
	for n, w := range p.pool {
		if w.endpoint == i {
			p.pool = append(p.pool[:n], p.pool[n+1:]...)
			found = w
			break
		}
	}
	p.mu.Unlock()
	if found != nil {
		found.cancel()
		select {
		case <-found.idleDone:
		case <-ctx.Done():
			found.abortNow()
			return nil
		}
		p.signal()
	}
	return found
}
func (p *Proxy) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *Proxy) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if p.pinned != nil {
		return p.dialPinned(ctx, network, addr)
	}
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("scfproxy: 不支持网络 %q", network)
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return nil, net.ErrClosed
	}
	callerCtx := ctx
	// 独立的超时原因区分内部拨号预算和调用方自己的截止时间。
	timeoutCause := errors.New("scfproxy: DialTimeout")
	ctx, cancel := context.WithTimeoutCause(ctx, p.cfg.DialTimeout, timeoutCause)
	defer cancel()
	stop := context.AfterFunc(p.ctx, cancel)
	defer stop()
	interrupted := func() error {
		if context.Cause(ctx) == timeoutCause {
			return nil
		}
		if err := callerCtx.Err(); err != nil {
			return err
		}
		if p.ctx.Err() != nil {
			return net.ErrClosed
		}
		return ctx.Err()
	}
	if p.bypass(host) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
	order := p.order(host)
	var last error = errors.New("scfproxy: 没有可用的 endpoint")
endpoints:
	for attempt, i := range order {
		if attempt > p.cfg.MaxRetries {
			break
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		w := p.take(ctx, i)
		fromPool := w != nil
		for {
			if w == nil {
				w, err = p.connect(ctx, i)
				if err != nil {
					if e := interrupted(); e != nil {
						return nil, e
					}
					last = err
					p.failed(i)
					continue endpoints
				}
			}
			req, e := protocol.NewRequest(p.cfg.Token, addr)
			if e == nil {
				var b []byte
				b, e = json.Marshal(req)
				if e == nil {
					e = w.ws.Write(ctx, websocket.MessageText, b)
				}
			}
			var reply protocol.Response
			if e == nil {
				select {
				case f, ok := <-w.frames:
					if !ok {
						e = errors.New("scfproxy: 握手连接关闭")
					} else if f.err != nil {
						e = f.err
					} else if f.typ != websocket.MessageText {
						e = errors.New("scfproxy: 握手响应必须为文本")
					} else {
						e = json.Unmarshal(f.data, &reply)
					}
				case <-ctx.Done():
					e = ctx.Err()
				}
			}
			if e != nil {
				if err := interrupted(); err != nil {
					w.abortNow()
					return nil, err
				}
				w.close()
				last = e
				if fromPool {
					// 池中连接可能刚被平台回收；先在同一出口重建，不消耗换出口次数。
					fromPool = false
					w = nil
					continue
				}
				p.failed(i)
				continue endpoints
			}
			if !reply.OK {
				w.ws.Close(websocket.StatusNormalClosure, "")
				w.close()
				return nil, &HandshakeError{Code: reply.Code, Message: reply.Error}
			}
			break
		}
		c := newTunnel(w, addr, p.cfg, func(c *tunnelConn) { p.mu.Lock(); delete(p.active, c); p.mu.Unlock() })
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			w.close()
			return nil, net.ErrClosed
		}
		p.cooldown[i] = time.Time{}
		p.active[c] = struct{}{}
		c.start()
		p.mu.Unlock()
		return c, nil
	}
	return nil, last
}
func (p *Proxy) failed(i int) {
	if p.cfg.Strategy == Pinned {
		return
	}
	p.mu.Lock()
	p.cooldown[i] = time.Now().Add(p.cfg.EndpointCooldown)
	p.mu.Unlock()
	if p.cfg.Logger != nil {
		p.cfg.Logger.Debug("出口进入冷却", "endpoint", i)
	}
}
func (p *Proxy) Transport() *http.Transport {
	t := directTransport()
	t.DialContext = p.DialContext
	t.ForceAttemptHTTP2 = true
	return t
}
func (p *Proxy) Close() error {
	if p.pinned != nil {
		return p.closePinned()
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	pool := p.pool
	p.pool = nil
	active := make([]*tunnelConn, 0, len(p.active))
	for c := range p.active {
		active = append(active, c)
	}
	p.mu.Unlock()
	for _, w := range pool {
		w.cancel()
		w.close()
		<-w.done
	}
	for _, c := range active {
		c.Close()
	}
	p.cancel()
	p.wg.Wait()
	return nil
}
func (p *Proxy) maintainPool() {
	defer p.wg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	next := 0
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}
		p.mu.Lock()
		now := time.Now()
		var expired []*warmConn
		kept := p.pool[:0]
		for _, w := range p.pool {
			dead := false
			select {
			case <-w.done:
				dead = true
			default:
			}
			if dead || now.Sub(w.born) >= p.cfg.PrewarmMaxIdle {
				expired = append(expired, w)
			} else {
				kept = append(kept, w)
			}
		}
		p.pool = kept
		need := p.cfg.Prewarm - len(p.pool)
		p.mu.Unlock()
		for _, w := range expired {
			w.cancel()
			w.close()
		}
		if need > 0 {
			i := next % len(p.endpoints)
			next++
			p.mu.Lock()
			healthy := p.cfg.Strategy == Pinned || !time.Now().Before(p.cooldown[i])
			p.mu.Unlock()
			if healthy {
				ctx, cancel := context.WithTimeout(p.ctx, p.cfg.DialTimeout)
				w, e := p.connect(ctx, i)
				cancel()
				if e != nil {
					p.failed(i)
				} else {
					wc, cc := context.WithCancel(p.ctx)
					w.cancel = cc
					w.idleDone = make(chan struct{})
					p.mu.Lock()
					if p.closed {
						p.mu.Unlock()
						cc()
						w.close()
						return
					}
					p.pool = append(p.pool, w)
					p.mu.Unlock()
					p.wg.Add(1)
					go p.keepWarm(wc, w)
				}
			}
		}
		select {
		case <-p.ctx.Done():
			return
		case <-p.wake:
		case <-ticker.C:
		}
	}
}
func (p *Proxy) keepWarm(ctx context.Context, w *warmConn) {
	defer p.wg.Done()
	defer close(w.idleDone)
	if p.cfg.PingInterval < 0 {
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(p.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pc, cancel := context.WithTimeout(p.ctx, min(p.cfg.PingInterval, 10*time.Second))
			err := w.ws.Ping(pc)
			cancel()
			if err != nil && ctx.Err() == nil {
				w.close()
				p.mu.Lock()
				for i, v := range p.pool {
					if v == w {
						p.pool = append(p.pool[:i], p.pool[i+1:]...)
						break
					}
				}
				p.mu.Unlock()
				p.signal()
				return
			}
		}
	}
}
