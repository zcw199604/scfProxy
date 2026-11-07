package scfproxy

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

type targetAddr string

func (a targetAddr) Network() string { return "tcp" }
func (a targetAddr) String() string  { return string(a) }

// tunnelConn 的 net.Pipe 提供并发及 deadline 语义，泵只持有一个消息大小的缓冲。
type tunnelConn struct {
	net.Conn
	peer    net.Conn
	w       *warmConn
	addr    string
	cfg     Config
	ctx     context.Context
	cancel  context.CancelFunc
	once    sync.Once
	upDone  chan struct{}
	done    chan struct{}
	closing atomic.Bool
	workers sync.WaitGroup
	onClose func(*tunnelConn)
}

func newTunnel(w *warmConn, addr string, cfg Config, onClose func(*tunnelConn)) *tunnelConn {
	a, b := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	return &tunnelConn{Conn: a, peer: b, w: w, addr: addr, cfg: cfg, ctx: ctx, cancel: cancel, upDone: make(chan struct{}), done: make(chan struct{}), onClose: onClose}
}
func (c *tunnelConn) RemoteAddr() net.Addr { return targetAddr(c.addr) }
func (c *tunnelConn) start() {
	c.workers.Add(2)
	go func() { defer c.workers.Done(); c.upload() }()
	go func() { defer c.workers.Done(); c.download() }()
	if c.cfg.PingInterval > 0 {
		c.workers.Add(1)
		go func() { defer c.workers.Done(); c.ping() }()
	}
}
func (c *tunnelConn) terminate() {
	c.once.Do(func() {
		c.w.close()
		c.cancel()
		c.peer.Close()
		close(c.done)
		c.onClose(c)
	})
}
func (c *tunnelConn) Close() error {
	c.closing.Store(true)
	c.Conn.Close()
	// EOF 让上行泵先发送已接受的缓冲；关闭不能无限等待平台限速或网络。
	select {
	case <-c.upDone:
	case <-c.done:
	case <-time.After(c.cfg.DialTimeout):
	}
	c.terminate()
	c.workers.Wait()
	<-c.w.done
	return nil
}
func (c *tunnelConn) upload() {
	defer close(c.upDone)
	defer c.terminate()
	buf := make([]byte, c.cfg.MaxMessageSize)
	used := 0
	var next time.Time
	flush := func() error {
		if used == 0 {
			return nil
		}
		if c.cfg.MaxMessagesPerSecond > 0 || c.cfg.MaxBytesPerSecond > 0 {
			// 容量为一的令牌桶，避免冷启动后的突发消息超过平台 QPS。
			if wait := time.Until(next); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-c.ctx.Done():
					timer.Stop()
					return c.ctx.Err()
				}
			}
			var spacing time.Duration
			if c.cfg.MaxMessagesPerSecond > 0 {
				spacing = time.Second / time.Duration(c.cfg.MaxMessagesPerSecond)
			}
			if c.cfg.MaxBytesPerSecond > 0 {
				spacing = max(spacing, time.Duration(float64(time.Second)*float64(used)/float64(c.cfg.MaxBytesPerSecond)))
			}
			next = time.Now().Add(spacing)
		}
		err := c.w.ws.Write(c.ctx, websocket.MessageBinary, buf[:used])
		used = 0
		return err
	}
	for {
		if used == 0 {
			c.peer.SetReadDeadline(time.Time{})
		}
		n, err := c.peer.Read(buf[used:])
		used += n
		if used > 0 && used == n {
			// 令牌尚未可用时继续接收小包，只有缓冲满才提前进入 flush 等待。
			deadline := time.Now().Add(c.cfg.CoalesceDelay)
			if next.After(deadline) {
				deadline = next
			}
			c.peer.SetReadDeadline(deadline)
		}
		if used == len(buf) || err != nil {
			if e := flush(); e != nil {
				return
			}
		}
		if err != nil {
			if e, ok := err.(net.Error); ok && e.Timeout() {
				continue
			}
			return
		}
	}
}
func (c *tunnelConn) download() {
	defer func() {
		if !c.closing.Load() {
			c.terminate()
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case f, ok := <-c.w.frames:
			if !ok || f.err != nil {
				return
			}
			if f.typ != websocket.MessageBinary {
				return
			}
			if _, err := c.peer.Write(f.data); err != nil {
				return
			}
		}
	}
}
func (c *tunnelConn) ping() {
	ticker := time.NewTicker(c.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(c.ctx, min(10*time.Second, c.cfg.PingInterval))
			err := c.w.ws.Ping(ctx)
			cancel()
			if err != nil {
				c.terminate()
				return
			}
		}
	}
}

var _ net.Conn = (*tunnelConn)(nil)
