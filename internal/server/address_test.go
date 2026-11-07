package server

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func TestForbiddenIP(t *testing.T) {
	for _, tt := range []struct {
		ip  string
		bad bool
	}{
		{"0.0.0.0", true}, {"127.0.0.1", true}, {"10.1.1.1", true}, {"172.16.0.1", true}, {"172.31.255.255", true}, {"192.168.0.1", true}, {"169.254.169.254", true}, {"100.64.0.1", true}, {"100.127.255.255", true}, {"224.0.0.1", true}, {"::", true}, {"::1", true}, {"fc00::1", true}, {"fdff::1", true}, {"fe80::1", true}, {"ff02::1", true}, {"::ffff:127.0.0.1", true}, {"::ffff:10.0.0.1", true}, {"8.8.8.8", false}, {"100.128.0.1", false}, {"172.32.0.1", false}, {"2001:4860:4860::8888", false},
	} {
		t.Run(tt.ip, func(t *testing.T) {
			if got := forbiddenIP(netip.MustParseAddr(tt.ip)); got != tt.bad {
				t.Fatalf("forbiddenIP=%v", got)
			}
		})
	}
}

func TestDialResolvedIPv4FirstAndPerIPBudget(t *testing.T) {
	s, _ := New(Config{Token: "secret", AllowPrivate: true})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ips := []netip.Addr{netip.MustParseAddr("2001:db8::1"), netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2")}
	var attempted []string
	c, err := s.dialResolved(ctx, ips, "443", func(ctx context.Context, network, addr string) (net.Conn, error) {
		attempted = append(attempted, addr)
		deadline, _ := ctx.Deadline()
		budget := time.Until(deadline)
		if len(attempted) == 1 && (budget > 2100*time.Millisecond || budget < 1900*time.Millisecond) {
			t.Errorf("首地址预算=%s", budget)
		}
		return nil, errors.New("network unreachable")
	})
	if c != nil || err == nil || len(attempted) != 3 {
		t.Fatalf("attempted=%v err=%v", attempted, err)
	}
	if attempted[0] != "192.0.2.1:443" || attempted[1] != "192.0.2.2:443" || attempted[2] != "[2001:db8::1]:443" {
		t.Fatalf("顺序=%v", attempted)
	}
	for _, addr := range attempted {
		if !strings.Contains(err.Error(), addr) {
			t.Fatalf("错误缺少地址 %s: %v", addr, err)
		}
	}
}

func TestDialResolvedFiltersAllBeforeDial(t *testing.T) {
	s, _ := New(Config{Token: "secret"})
	_, err := s.dialResolved(context.Background(), []netip.Addr{netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("169.254.169.254")}, "443", func(context.Context, string, string) (net.Conn, error) {
		t.Fatal("过滤前不应拨号")
		return nil, nil
	})
	if !errors.Is(err, errForbidden) || !strings.Contains(err.Error(), "169.254.169.254") || !strings.Contains(err.Error(), "link-local") {
		t.Fatalf("缺少拦截诊断: %v", err)
	}
}

func TestDialTargetDNSFailureIsDiagnostic(t *testing.T) {
	// 所有 DNS 传输均由本地假 Dial 拒绝，不发送任何网络请求。
	saved := net.DefaultResolver
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("local DNS transport unavailable")
	}}
	defer func() { net.DefaultResolver = saved }()
	s, _ := New(Config{Token: "secret"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := s.dialTarget(ctx, "dns-failure.example.test:443")
	if err == nil || !strings.Contains(err.Error(), "DNS lookup dns-failure.example.test") || !strings.Contains(err.Error(), "local DNS transport unavailable") {
		t.Fatalf("DNS错误缺少诊断: %v", err)
	}
}

func TestDialResolvedContinuesAfterPerIPTimeout(t *testing.T) {
	s, _ := New(Config{Token: "secret", AllowPrivate: true})
	ctx, cancel := context.WithTimeout(context.Background(), 2200*time.Millisecond)
	defer cancel()
	calls := 0
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	c, err := s.dialResolved(ctx, []netip.Addr{netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2")}, "443", func(attempt context.Context, _, addr string) (net.Conn, error) {
		calls++
		if calls == 1 {
			<-attempt.Done()
			return nil, attempt.Err()
		}
		if attempt.Err() != nil || addr != "192.0.2.2:443" {
			t.Fatalf("未保留下一个地址预算: %s %v", addr, attempt.Err())
		}
		return client, nil
	})
	if err != nil || c != client || calls != 2 {
		t.Fatalf("超时后没有回退成功: calls=%d err=%v", calls, err)
	}
}
