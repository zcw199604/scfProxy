package protocol

import (
	"strings"
	"testing"
	"time"
)

func TestSignAndVerify(t *testing.T) {
	r, err := NewRequest("secret", "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	cases := []struct {
		name   string
		mutate func(*Request)
		token  string
		valid  bool
	}{
		{"正常", func(*Request) {}, "secret", true},
		{"错误令牌", func(*Request) {}, "wrong", false},
		{"签名篡改", func(r *Request) { r.Sign = strings.Repeat("0", 64) }, "secret", false},
		{"版本错误", func(r *Request) { r.V = 2 }, "secret", false},
		{"过期", func(r *Request) { r.TS = now.Add(-301 * time.Second).Unix(); r.Sign = Sign("secret", *r) }, "secret", false},
		{"未来", func(r *Request) { r.TS = now.Add(301 * time.Second).Unix(); r.Sign = Sign("secret", *r) }, "secret", false},
		{"边界", func(r *Request) { r.TS = now.Add(-300 * time.Second).Unix(); r.Sign = Sign("secret", *r) }, "secret", true},
		{"错误nonce", func(r *Request) { r.Nonce = "x"; r.Sign = Sign("secret", *r) }, "secret", false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := r
			tt.mutate(&c)
			if got := Verify(tt.token, c, now); got != tt.valid {
				t.Fatalf("Verify=%v", got)
			}
		})
	}
}
