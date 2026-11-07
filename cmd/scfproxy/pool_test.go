package main

import (
	"bytes"
	"context"
	"github.com/zcw199604/scfproxy/internal/poolfile"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPoolHelpAndLocalStatus(t *testing.T) {
	for _, command := range []string{"init", "status", "switch", "unban", "replenish", "rotate", "destroy"} {
		var out bytes.Buffer
		if err := run(context.Background(), []string{"pool", command, "-h"}, strings.NewReader(""), &out, false); err != nil {
			t.Fatalf("%s: %v", command, err)
		}
		if out.Len() == 0 {
			t.Fatal("帮助为空")
		}
	}
	var out bytes.Buffer
	if err := run(context.Background(), []string{"pool", "status", "--pool", filepath.Join(t.TempDir(), "pool.json")}, strings.NewReader(""), &out, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "主用") {
		t.Fatalf("缺少池状态: %s", out.String())
	}
}
func TestPoolMutationsRequireConfirmation(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{"pool", "destroy", "--pool", filepath.Join(t.TempDir(), "pool.json")}, strings.NewReader(""), &out, false)
	if err == nil || !strings.Contains(err.Error(), "--yes") {
		t.Fatalf("必须先确认: %v", err)
	}
}

func TestPoolSwitchIsLocalAndRetiresPrimary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.json")
	err := poolfile.Update(path, func(s *poolfile.State) error {
		s.Active = []poolfile.Endpoint{
			{URL: "wss://primary.example.test", Region: "ap-singapore", Function: "scfproxy-a", EgressIP: "192.0.2.1", CreatedAt: time.Now()},
			{URL: "wss://standby.example.test", Region: "ap-tokyo", Function: "scfproxy-b", EgressIP: "192.0.2.2", CreatedAt: time.Now()},
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = run(context.Background(), []string{"pool", "switch", "--pool", path, "--reason", "blocked", "--ban-ttl", "30m"}, strings.NewReader(""), &out, false); err != nil {
		t.Fatal(err)
	}
	s, err := poolfile.Read(path)
	if err != nil || len(s.Active) != 1 || s.Active[0].EgressIP != "192.0.2.2" || len(s.Retired) != 1 || s.Retired[0].Reason != "blocked" || len(s.BannedIPs) != 1 {
		t.Fatalf("state=%+v err=%v", s, err)
	}
	if s.BannedIPs[0].ExpiresAt.Sub(s.BannedIPs[0].BannedAt) != 30*time.Minute {
		t.Fatalf("CLI未使用ban-ttl: %+v", s.BannedIPs)
	}
}

func TestPoolUnbanLocalAndExclusiveFlags(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		want    int
		invalid bool
	}{
		{"ip", []string{"--ip", "192.0.2.1"}, 1, false},
		{"all", []string{"--all"}, 0, false},
		{"missing", nil, 2, true},
		{"both", []string{"--ip", "192.0.2.1", "--all"}, 2, true},
		{"invalid", []string{"--ip", "invalid"}, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pool.json")
			if err := poolfile.Update(path, func(s *poolfile.State) error {
				for _, ip := range []string{"192.0.2.1", "192.0.2.2"} {
					s.BannedIPs = append(s.BannedIPs, poolfile.Ban{IP: ip, BannedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)})
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			args := append([]string{"pool", "unban", "--pool", path}, tc.args...)
			err := run(context.Background(), args, strings.NewReader(""), &out, false)
			if (err != nil) != tc.invalid {
				t.Fatalf("err=%v", err)
			}
			s, err := poolfile.Read(path)
			if err != nil || len(s.BannedIPs) != tc.want {
				t.Fatalf("state=%+v err=%v", s, err)
			}
			if tc.name == "ip" && s.BannedIPs[0].IP != "192.0.2.2" {
				t.Fatal("误删其他IP")
			}
		})
	}
}

func TestPoolStatusShowsBanRemainingTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.json")
	if err := poolfile.Update(path, func(s *poolfile.State) error {
		s.BannedIPs = []poolfile.Ban{{IP: "192.0.2.1", BannedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour), Reason: "blocked"}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run(context.Background(), []string{"pool", "status", "--pool", path}, strings.NewReader(""), &out, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "192.0.2.1 剩余=") {
		t.Fatal(out.String())
	}
}
