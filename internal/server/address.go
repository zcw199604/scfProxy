package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"time"
)

var carrierNAT = netip.MustParsePrefix("100.64.0.0/10")

func forbiddenReason(ip netip.Addr) string {
	ip = ip.Unmap()
	switch {
	case !ip.IsValid():
		return "invalid"
	case ip.IsUnspecified():
		return "unspecified"
	case ip.IsLoopback():
		return "loopback"
	case ip.IsPrivate():
		if ip.Is4() {
			return "RFC1918 private"
		}
		return "IPv6 ULA fc00::/7"
	case ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast():
		return "link-local"
	case ip.IsMulticast():
		return "multicast"
	case carrierNAT.Contains(ip):
		return "carrier-grade NAT 100.64.0.0/10"
	}
	return ""
}
func forbiddenIP(ip netip.Addr) bool { return forbiddenReason(ip) != "" }

var errForbidden = errors.New("目标地址不允许")
var errBadTarget = errors.New("目标必须是合法的 host:port")

func (s *Server) dialTarget(ctx context.Context, target string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil || host == "" {
		return nil, errBadTarget
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return nil, errBadTarget
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("DNS lookup %s: %w", host, err)
	}
	if len(ips) == 0 {
		return nil, errors.New("目标没有解析地址")
	}
	return s.dialResolved(ctx, ips, port, (&net.Dialer{}).DialContext)
}

func (s *Server) dialResolved(ctx context.Context, ips []netip.Addr, port string, dial func(context.Context, string, string) (net.Conn, error)) (net.Conn, error) {
	// 必须检查全部结果后再拨号，避免混合公网与内网的 DNS 结果绕过校验。
	for _, ip := range ips {
		if !s.cfg.AllowPrivate {
			if reason := forbiddenReason(ip); reason != "" {
				return nil, fmt.Errorf("%w: resolved IP %s matches %s", errForbidden, ip, reason)
			}
		}
	}
	ips = append([]netip.Addr(nil), ips...)
	sort.SliceStable(ips, func(i, j int) bool { return ips[i].Unmap().Is4() && !ips[j].Unmap().Is4() })
	var failures []error
	for i, ip := range ips {
		if err := ctx.Err(); err != nil {
			failures = append(failures, err)
			break
		}
		attemptCtx := ctx
		cancel := func() {}
		if deadline, ok := ctx.Deadline(); ok {
			remaining := time.Until(deadline)
			budget := min(remaining, max(remaining/time.Duration(len(ips)-i), 2*time.Second))
			attemptCtx, cancel = context.WithTimeout(ctx, budget)
		}
		addr := net.JoinHostPort(ip.Unmap().String(), port)
		c, err := dial(attemptCtx, "tcp", addr)
		cancel()
		if err == nil {
			return c, nil
		}
		failures = append(failures, fmt.Errorf("%s: %w", addr, err))
	}
	if len(failures) == 0 {
		return nil, errors.New("目标没有解析地址")
	}
	return nil, errors.Join(failures...)
}

// diagnostic 同时用于握手回复与日志，去除任何可能由目标或底层错误回显的认证值。
func (s *Server) diagnostic(message string, sign string) string {
	for _, secret := range []string{sign, s.cfg.Token} {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[REDACTED]")
		}
	}
	return message
}
