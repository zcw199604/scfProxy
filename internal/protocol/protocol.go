// Package protocol 定义隧道带内握手与签名。
package protocol

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

type Request struct {
	V      int    `json:"v"`
	Target string `json:"target"`
	TS     int64  `json:"ts"`
	Nonce  string `json:"nonce"`
	Sign   string `json:"sign"`
}

type Response struct {
	OK       bool   `json:"ok"`
	Instance string `json:"instance,omitempty"`
	Active   int    `json:"active,omitempty"`
	Code     string `json:"code,omitempty"`
	Error    string `json:"error,omitempty"`
}

func NewRequest(token, target string) (Request, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return Request{}, err
	}
	r := Request{V: 1, Target: target, TS: time.Now().Unix(), Nonce: hex.EncodeToString(nonce[:])}
	r.Sign = Sign(token, r)
	return r, nil
}

// Sign 使用 UTF-8 原文 v|target|ts|nonce，其中版本和时间为十进制。
func Sign(token string, r Request) string {
	mac := hmac.New(sha256.New, []byte(token))
	fmt.Fprintf(mac, "%d|%s|%d|%s", r.V, r.Target, r.TS, r.Nonce)
	return hex.EncodeToString(mac.Sum(nil))
}

func Verify(token string, r Request, now time.Time) bool {
	if token == "" || r.V != 1 || r.Target == "" || r.TS < now.Unix()-300 || r.TS > now.Unix()+300 {
		return false
	}
	nonce, err := hex.DecodeString(r.Nonce)
	if err != nil || len(nonce) != 16 {
		return false
	}
	actual, err := hex.DecodeString(r.Sign)
	if err != nil {
		return false
	}
	expected, _ := hex.DecodeString(Sign(token, r))
	return hmac.Equal(actual, expected)
}
