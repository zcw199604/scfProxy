package server

import (
	"net/netip"
	"testing"
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
