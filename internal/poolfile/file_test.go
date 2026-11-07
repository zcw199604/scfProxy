package poolfile

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestLegacyBansConvertOnWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.json")
	legacy := []byte(`{"version":1,"active":[],"retired":[],"banned_ips":["192.0.2.99"]}`)
	if err := os.WriteFile(path, legacy, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(path); err != nil {
		t.Fatal(err)
	}
	s, err := Read(path)
	if err != nil || len(s.BannedIPs) != 1 || time.Since(s.BannedIPs[0].BannedAt) > time.Second || s.BannedIPs[0].ExpiresAt.Sub(s.BannedIPs[0].BannedAt) != DefaultBanTTL {
		t.Fatalf("旧记录未按读取时间转换: %+v %v", s, err)
	}
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got, legacy) {
		t.Fatal("读取修改了文件")
	}
	if err := Update(path, func(*State) error { return nil }); err != nil {
		t.Fatal(err)
	}
	got, _ = os.ReadFile(path)
	if !bytes.Contains(got, []byte(`"expires_at"`)) {
		t.Fatalf("未转换旧封禁记录: %s", got)
	}
}

func TestExpiredBansRemainOnReadAndArePrunedOnWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.json")
	now := time.Now().UTC()
	s := empty()
	s.Active = []Endpoint{endpoint("a")}
	s.BannedIPs = []Ban{{IP: s.Active[0].EgressIP, BannedAt: now.Add(-2 * time.Hour), ExpiresAt: now.Add(-time.Hour)}}
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, b, 0644); err != nil {
		t.Fatal(err)
	}
	read, err := Read(path)
	if err != nil || len(read.BannedIPs) != 1 {
		t.Fatalf("过期封禁影响读取: %+v %v", read, err)
	}
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got, b) {
		t.Fatal("读取修改了文件")
	}
	if err = Update(path, func(*State) error { return nil }); err != nil {
		t.Fatal(err)
	}
	read, err = Read(path)
	if err != nil || len(read.BannedIPs) != 0 {
		t.Fatalf("写入未清理: %+v %v", read, err)
	}
}

func TestRepeatedBanRefreshesWithoutShorteningExpiry(t *testing.T) {
	now := time.Now().UTC()
	s := empty()
	banIP(&s, "192.0.2.1", "first", now, 6*time.Hour)
	banIP(&s, "192.0.2.1", "second", now.Add(time.Hour), time.Hour)
	if len(s.BannedIPs) != 1 || !s.BannedIPs[0].ExpiresAt.Equal(now.Add(6*time.Hour)) || !s.BannedIPs[0].BannedAt.Equal(now.Add(time.Hour)) || s.BannedIPs[0].Reason != "second" {
		t.Fatalf("重复封禁记录错误: %+v", s.BannedIPs)
	}
	banIP(&s, "192.0.2.1", "third", now.Add(2*time.Hour), 8*time.Hour)
	if len(s.BannedIPs) != 1 || !s.BannedIPs[0].ExpiresAt.Equal(now.Add(10*time.Hour)) {
		t.Fatalf("未延长封禁: %+v", s.BannedIPs)
	}
}

func TestSwitchBanTTL(t *testing.T) {
	for _, ttl := range []time.Duration{0, 30 * time.Minute, -1} {
		t.Run(ttl.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pool.json")
			if err := Update(path, func(s *State) error { s.Active = []Endpoint{endpoint("a"), endpoint("b")}; return nil }); err != nil {
				t.Fatal(err)
			}
			if _, _, err := Switch(path, "blocked", ttl); err != nil {
				t.Fatal(err)
			}
			s, err := Read(path)
			if err != nil {
				t.Fatal(err)
			}
			if ttl < 0 {
				if len(s.BannedIPs) != 0 {
					t.Fatal("负TTL仍记录封禁")
				}
				return
			}
			want := ttl
			if want == 0 {
				want = DefaultBanTTL
			}
			if len(s.BannedIPs) != 1 || s.BannedIPs[0].ExpiresAt.Sub(s.BannedIPs[0].BannedAt) != want || s.BannedIPs[0].Reason != "blocked" || !s.BannedIPs[0].BannedAt.Equal(s.Retired[0].RetiredAt) {
				t.Fatalf("封禁有效期错误: %+v", s)
			}
		})
	}
}

func endpoint(i string) Endpoint {
	return Endpoint{URL: "ws://" + i + ".test", Region: "ap-" + i, Function: "scfproxy-ap-" + i + "-1", EgressIP: "192.0.2." + map[string]string{"a": "1", "b": "2", "c": "3"}[i], CreatedAt: time.Now().UTC()}
}
func TestSwitchAtomicNoStandbyAndConcurrentUpdates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.json")
	if err := Update(path, func(s *State) error { s.Active = []Endpoint{endpoint("a")}; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Switch(path, "blocked"); !errors.Is(err, ErrNoStandby) {
		t.Fatal(err)
	}
	s, _ := Read(path)
	if len(s.Retired) != 0 || len(s.BannedIPs) != 0 {
		t.Fatal("无备用改变状态")
	}
	Update(path, func(s *State) error { s.Active = append(s.Active, endpoint("b")); return nil })
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if _, _, err := Switch(path, "blocked"); err != nil {
			t.Error(err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := Update(path, func(s *State) error { s.Active = append(s.Active, endpoint("c")); return nil }); err != nil {
			t.Error(err)
		}
	}()
	wg.Wait()
	s, err := Read(path)
	if err != nil || len(s.Active) != 2 || len(s.Retired) != 1 || len(s.BannedIPs) != 1 || s.Active[0].Region != "ap-b" {
		t.Fatalf("state=%+v err=%v", s, err)
	}
}
