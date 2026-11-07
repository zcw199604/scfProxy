package scfproxy

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/zcw199604/scfproxy/internal/poolfile"
)

// EndpointInfo 是当前主用的非敏感信息。
type EndpointInfo struct{ URL, Region, Function, EgressIP string }

var ErrNoStandby = poolfile.ErrNoStandby

type pinnedState struct {
	change sync.Mutex
	child  *Proxy
	info   EndpointInfo
}

func endpointInfo(e poolfile.Endpoint) EndpointInfo {
	return EndpointInfo{URL: e.URL, Region: e.Region, Function: e.Function, EgressIP: e.EgressIP}
}

func newPinnedProxy(cfg Config) (*Proxy, error) {
	if len(cfg.Endpoints) > 0 || cfg.EndpointsFile != "" {
		return nil, errors.New("scfproxy: PoolFile 与 Endpoints/EndpointsFile 互斥")
	}
	if cfg.ReloadInterval == 0 {
		cfg.ReloadInterval = 2 * time.Second
	}
	if cfg.ReloadInterval < 0 {
		return nil, errors.New("scfproxy: ReloadInterval 必须为正数")
	}
	state, err := poolfile.Read(cfg.PoolFile)
	if err != nil {
		return nil, err
	}
	if len(state.Active) == 0 {
		return nil, errors.New("scfproxy: 固定IP池没有主用出口")
	}
	info := endpointInfo(state.Active[0])
	child, err := newPinnedChild(cfg, info)
	if err != nil {
		return nil, err
	}
	path := cfg.PoolFile
	cfg = child.cfg
	cfg.PoolFile = path
	cfg.Endpoints = nil
	ctx, cancel := context.WithCancel(context.Background())
	p := &Proxy{cfg: cfg, ctx: ctx, cancel: cancel, pinned: &pinnedState{child: child, info: info}}
	p.wg.Add(1)
	go p.watchPool()
	return p, nil
}

func newPinnedChild(cfg Config, info EndpointInfo) (*Proxy, error) {
	cfg.PoolFile = ""
	cfg.Endpoints = []string{info.URL}
	cfg.EndpointsFile = ""
	cfg.Strategy = Pinned
	return New(cfg)
}
func (p *Proxy) Active() EndpointInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pinned == nil {
		return EndpointInfo{}
	}
	return p.pinned.info
}
func (p *Proxy) dialPinned(ctx context.Context, network, addr string) (net.Conn, error) {
	for attempt := 0; ; attempt++ {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, net.ErrClosed
		}
		child := p.pinned.child
		p.mu.Unlock()
		if child == nil {
			return nil, errors.New("scfproxy: 固定IP池没有主用出口")
		}
		conn, err := child.DialContext(ctx, network, addr)
		p.mu.Lock()
		closed := p.closed
		current := p.pinned.child == child
		p.mu.Unlock()
		if closed || !current {
			if conn != nil {
				conn.Close()
			}
			if closed {
				return nil, net.ErrClosed
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			// 仅在显式切换或热加载已经更换主用后，额外使用新主用重试一次。
			if attempt == 0 {
				continue
			}
			return nil, errors.New("scfproxy: 拨号期间主用出口已切换")
		}
		return conn, err
	}
}
func (p *Proxy) Switch(reason string) (old, new EndpointInfo, err error) {
	if p.pinned == nil {
		return old, new, errors.New("scfproxy: Switch 需要 PoolFile")
	}
	p.pinned.change.Lock()
	defer p.pinned.change.Unlock()
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return old, new, net.ErrClosed
	}
	previous, next, err := poolfile.Switch(p.cfg.PoolFile, reason, p.cfg.BanTTL)
	if err != nil {
		return old, new, err
	}
	old, new = endpointInfo(previous), endpointInfo(next)
	return old, new, p.applyPinned(new, reason)
}

// applyPinned 的调用由 change 串行化；先发布新代，再关闭旧代，避免关闭期间新的拨号仍取得旧代。
func (p *Proxy) applyPinned(info EndpointInfo, reason string) error {
	p.mu.Lock()
	oldInfo := p.pinned.info
	same := oldInfo == info
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return net.ErrClosed
	}
	if same {
		return nil
	}
	var child *Proxy
	var err error
	if info.URL != "" {
		child, err = newPinnedChild(p.cfg, info)
		if err != nil {
			return err
		}
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		if child != nil {
			child.Close()
		}
		return net.ErrClosed
	}
	old := p.pinned.child
	p.pinned.child = child
	p.pinned.info = info
	p.mu.Unlock()
	if old != nil {
		// 显式换 IP 不等待旧连接 flush，立即中断旧代的活动、预热和在途连接。
		old.cancel()
		old.Close()
	}
	if callback := p.cfg.OnSwitch; callback != nil {
		go callback(oldInfo, info, reason)
	}
	return nil
}
func (p *Proxy) watchPool() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.cfg.ReloadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.pinned.change.Lock()
			state, err := poolfile.Read(p.cfg.PoolFile)
			if err == nil {
				var info EndpointInfo
				if len(state.Active) > 0 {
					info = endpointInfo(state.Active[0])
				}
				err = p.applyPinned(info, "external")
			}
			p.pinned.change.Unlock()
			if err != nil && p.ctx.Err() == nil && p.cfg.Logger != nil {
				p.cfg.Logger.Warn("固定IP池加载失败", "error", err)
			}
		}
	}
}
func (p *Proxy) closePinned() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.cancel()
	child := p.pinned.child
	p.pinned.child = nil
	p.mu.Unlock()
	if child != nil {
		child.Close()
	}
	p.wg.Wait()
	return nil
}
