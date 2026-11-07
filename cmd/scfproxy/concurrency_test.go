package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zcw199604/scfproxy/internal/server"
)

func TestConcurrencyCommandUsesFirstEndpointAndSummarizes(t *testing.T) {
	t.Setenv("SCFPROXY_TOKEN", "local-test-token")
	h, err := server.New(server.Config{Token: "local-test-token", AllowPrivate: true})
	if err != nil {
		t.Fatal(err)
	}
	tunnel := httptest.NewServer(h)
	defer tunnel.Close()
	defer h.Close()
	var otherConnections atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { otherConnections.Add(1); http.Error(w, "unused", 503) }))
	defer other.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer target.Close()
	path := filepath.Join(t.TempDir(), "endpoints.txt")
	if err = os.WriteFile(path, []byte("ws"+strings.TrimPrefix(tunnel.URL, "http")+"\nws"+strings.TrimPrefix(other.URL, "http")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var out bytes.Buffer
	err = run(ctx, []string{"concurrency", "-f", path, "-n", "5", "--hold", "20ms", "--target", strings.TrimPrefix(target.URL, "http://")}, strings.NewReader(""), &out, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"不同实例数=1", "隧道数=5", "最大active=5", "失败=0"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("缺少%s: %s", want, out.String())
		}
	}
	if otherConnections.Load() != 0 {
		t.Fatal("连接了第二个endpoint")
	}
}

func TestMaxConcurrencyCLIRejectsOutOfRange(t *testing.T) {
	t.Setenv("SCFPROXY_TOKEN", "local-test-token")
	for _, args := range [][]string{
		{"deploy", "-r", "ap-shanghai", "--max-concurrency", "101"},
		{"pool", "init", "--regions", "ap-shanghai,ap-beijing", "--max-concurrency", "-1"},
		{"pool", "replenish", "--regions", "ap-shanghai,ap-beijing", "--max-concurrency", "101"},
		{"pool", "rotate", "--regions", "ap-shanghai,ap-beijing", "--max-concurrency", "101"},
	} {
		var out bytes.Buffer
		err := run(context.Background(), args, strings.NewReader(""), &out, false)
		if err == nil || !strings.Contains(err.Error(), "max-concurrency") {
			t.Fatalf("%v: %v", args, err)
		}
	}
}
