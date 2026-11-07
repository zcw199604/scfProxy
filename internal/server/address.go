package server

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
)

var carrierNAT = netip.MustParsePrefix("100.64.0.0/10")

func forbiddenIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	return !ip.IsValid() || ip.IsUnspecified() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || carrierNAT.Contains(ip)
}

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
		return nil, err
	}
	if len(ips) == 0 {
		return nil, errors.New("目标没有解析地址")
	}
	// 必须检查全部结果后再拨号，避免混合公网与内网的 DNS 结果绕过校验。
	for _, ip := range ips {
		if !s.cfg.AllowPrivate && forbiddenIP(ip) {
			return nil, errForbidden
		}
	}
	var last error
	for _, ip := range ips {
		c, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(ip.Unmap().String(), port))
		if err == nil {
			return c, nil
		}
		last = err
	}
	return nil, last
}
