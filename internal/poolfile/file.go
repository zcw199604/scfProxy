// Package poolfile 提供跨进程互斥的固定出口池文件事务。
package poolfile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

type Endpoint struct {
	URL       string    `json:"url"`
	Region    string    `json:"region"`
	Function  string    `json:"function"`
	EgressIP  string    `json:"egress_ip"`
	CreatedAt time.Time `json:"created_at"`
}
type Retired struct {
	Endpoint
	RetiredAt time.Time `json:"retired_at"`
	Reason    string    `json:"reason"`
}

const DefaultBanTTL = 24 * time.Hour

// Ban 是临时封禁记录。旧格式在读取时使用默认有效期转换，不立即写回。
type Ban struct {
	IP        string    `json:"ip"`
	BannedAt  time.Time `json:"banned_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Reason    string    `json:"reason"`
}

func (b *Ban) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		var ip string
		if err := json.Unmarshal(data, &ip); err != nil {
			return err
		}
		now := time.Now().UTC()
		*b = Ban{IP: ip, BannedAt: now, ExpiresAt: now.Add(DefaultBanTTL)}
		return nil
	}
	type plain Ban
	return json.Unmarshal(data, (*plain)(b))
}

type State struct {
	Version        int                  `json:"version"`
	Active         []Endpoint           `json:"active"`
	Retired        []Retired            `json:"retired"`
	BannedIPs      []Ban                `json:"banned_ips"`
	RegionLastUsed map[string]time.Time `json:"region_last_used,omitempty"`
}

var ErrNoStandby = errors.New("scfpool: 没有备用出口")
var regionPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)+$`)
var functionPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]*[A-Za-z0-9]$`)

func validIdentity(e Endpoint) bool {
	return regionPattern.MatchString(e.Region) && len(e.Function) <= 60 && functionPattern.MatchString(e.Function)
}

func empty() State {
	return State{Version: 1, Active: []Endpoint{}, Retired: []Retired{}, BannedIPs: []Ban{}, RegionLastUsed: map[string]time.Time{}}
}
func read(path string) (State, error) {
	s := empty()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return s, err
	}
	if err = json.Unmarshal(b, &s); err != nil {
		return s, err
	}
	if s.RegionLastUsed == nil {
		s.RegionLastUsed = map[string]time.Time{}
	}
	return s, validate(s)
}
func validate(s State) error {
	if s.Version != 1 {
		return errors.New("scfpool: 不支持的池文件版本")
	}
	regions, ips, functions := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, e := range s.Active {
		u, err := url.Parse(e.URL)
		if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" || (u.Scheme != "ws" && u.Scheme != "wss") {
			return errors.New("scfpool: 无效出口 URL")
		}
		ip, err := netip.ParseAddr(e.EgressIP)
		if err != nil || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			return errors.New("scfpool: 无效出口 IP")
		}
		if !validIdentity(e) {
			return errors.New("scfpool: 缺少地域或函数名")
		}
		key := e.Region + "/" + e.Function
		if regions[e.Region] || ips[ip.Unmap().String()] || functions[key] {
			return errors.New("scfpool: active 中地域、函数或出口 IP 重复")
		}
		regions[e.Region] = true
		ips[ip.Unmap().String()] = true
		functions[key] = true
	}
	for _, ban := range s.BannedIPs {
		if _, err := netip.ParseAddr(ban.IP); err != nil {
			return fmt.Errorf("scfpool: 无效 banned IP: %q", ban.IP)
		}
	}
	for _, e := range s.Active {
		for _, ban := range s.BannedIPs {
			if !ban.ExpiresAt.After(time.Now()) {
				continue
			}
			a, _ := netip.ParseAddr(e.EgressIP)
			b, _ := netip.ParseAddr(ban.IP)
			if a.Unmap() == b.Unmap() {
				return errors.New("scfpool: active IP 已被封禁")
			}
		}
	}
	for _, r := range s.Retired {
		if !validIdentity(r.Endpoint) {
			return errors.New("scfpool: retired 缺少地域或函数名")
		}
	}
	return nil
}
func Read(path string) (State, error) {
	unlock, err := Lock(context.Background(), path+".lock")
	if err != nil {
		return State{}, err
	}
	defer unlock()
	return read(path)
}
func Update(path string, fn func(*State) error) error {
	unlock, err := Lock(context.Background(), path+".lock")
	if err != nil {
		return err
	}
	defer unlock()
	s, err := read(path)
	if err != nil {
		return err
	}
	if err = fn(&s); err != nil {
		return err
	}
	now := time.Now()
	kept := s.BannedIPs[:0]
	for _, ban := range s.BannedIPs {
		if ban.ExpiresAt.After(now) {
			kept = append(kept, ban)
		}
	}
	s.BannedIPs = kept
	if err = validate(s); err != nil {
		return err
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	f, err := os.CreateTemp(filepath.Dir(path), ".scfpool-*")
	if err != nil {
		return err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if err = f.Chmod(0644); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(temp, path)
}

// Switch 的可选 ttl 为零或未指定时使用24小时，负数表示不记录封禁。
func Switch(path, reason string, ttl ...time.Duration) (old, new Endpoint, err error) {
	banTTL := DefaultBanTTL
	if len(ttl) > 0 && ttl[0] != 0 {
		banTTL = ttl[0]
	}
	err = Update(path, func(s *State) error {
		if len(s.Active) < 2 {
			return ErrNoStandby
		}
		old = s.Active[0]
		new = s.Active[1]
		now := time.Now().UTC()
		s.Active = s.Active[1:]
		s.Retired = append(s.Retired, Retired{Endpoint: old, RetiredAt: now, Reason: reason})
		s.RegionLastUsed[old.Region] = now
		if banTTL > 0 {
			banIP(s, old.EgressIP, reason, now, banTTL)
		}
		return nil
	})
	return
}

func banIP(s *State, ip, reason string, now time.Time, ttl time.Duration) {
	ban := Ban{IP: ip, BannedAt: now, ExpiresAt: now.Add(ttl), Reason: reason}
	for i, previous := range s.BannedIPs {
		a, _ := netip.ParseAddr(previous.IP)
		b, _ := netip.ParseAddr(ip)
		if a.Unmap() == b.Unmap() {
			if previous.ExpiresAt.After(ban.ExpiresAt) {
				ban.ExpiresAt = previous.ExpiresAt
			}
			s.BannedIPs[i] = ban
			return
		}
	}
	s.BannedIPs = append(s.BannedIPs, ban)
}

// Unban 只修改本地文件；ip为空表示解除全部封禁。
func Unban(path, ip string) error {
	var target netip.Addr
	if ip != "" {
		var err error
		target, err = netip.ParseAddr(ip)
		if err != nil {
			return fmt.Errorf("scfpool: 无效 IP: %w", err)
		}
	}
	return Update(path, func(s *State) error {
		kept := s.BannedIPs[:0]
		for _, ban := range s.BannedIPs {
			addr, _ := netip.ParseAddr(ban.IP)
			if ip != "" && addr.Unmap() != target.Unmap() {
				kept = append(kept, ban)
			}
		}
		s.BannedIPs = kept
		return nil
	})
}
