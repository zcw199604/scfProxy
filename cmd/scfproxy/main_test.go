package main

import (
	"archive/zip"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestConcurrencyPlanDefaultsAndExplicitZero(t *testing.T) {
	t.Setenv("SCFPROXY_TOKEN", "local-test-token")
	var archive bytes.Buffer
	z := zip.NewWriter(&archive)
	for _, name := range []string{"scf-server", "scf_bootstrap"} {
		h := &zip.FileHeader{Name: name}
		h.SetMode(0755)
		w, err := z.CreateHeader(h)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.Write([]byte("test")); err != nil {
			t.Fatal(err)
		}
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "server.zip")
	if err := os.WriteFile(path, archive.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	for _, base := range [][]string{
		{"deploy", "-r", "ap-guangzhou"},
		{"deploy", "-r", "ap-guangzhou", "--update-config"},
		{"pool", "init", "--regions", "ap-shanghai,ap-beijing"},
		{"pool", "replenish", "--regions", "ap-shanghai,ap-beijing"},
		{"pool", "rotate", "--regions", "ap-shanghai,ap-beijing"},
	} {
		for _, value := range []string{"default", "0"} {
			args := append(append([]string{}, base...), "--package", path)
			want := "10"
			if value == "0" {
				args = append(args, "--max-concurrency", "0")
				want = "0"
			}
			var out bytes.Buffer
			err := run(context.Background(), args, strings.NewReader(""), &out, false)
			// 非交互拒绝确认，确保此测试永不调用云API。
			if err == nil || !strings.Contains(err.Error(), "--yes") {
				t.Fatalf("%v: %v", args, err)
			}
			label := "max-concurrency="
			if base[0] == "pool" {
				label = "单实例并发="
			}
			if !strings.Contains(out.String(), label+want) {
				t.Fatalf("%v: %s", args, out.String())
			}
		}
	}
}

func TestFunctionMemoryDefaultsTo64MB(t *testing.T) {
	for _, args := range [][]string{{"deploy", "-h"}, {"pool", "init", "-h"}, {"pool", "replenish", "-h"}, {"pool", "rotate", "-h"}} {
		var out bytes.Buffer
		if err := run(context.Background(), args, strings.NewReader(""), &out, false); err != nil {
			t.Fatal(err)
		}
		if !regexp.MustCompile(`-memory int\n[^\n]*\(default 64\)`).MatchString(out.String()) {
			t.Errorf("%v 的memory默认值不是64MB: %s", args, out.String())
		}
	}
}

func TestInstanceConcurrencyDefaultsTo10(t *testing.T) {
	for _, args := range [][]string{{"deploy", "-h"}, {"pool", "init", "-h"}, {"pool", "replenish", "-h"}, {"pool", "rotate", "-h"}} {
		var out bytes.Buffer
		if err := run(context.Background(), args, strings.NewReader(""), &out, false); err != nil {
			t.Fatal(err)
		}
		if !regexp.MustCompile(`-max-concurrency int\n[^\n]*\(default 10\)`).MatchString(out.String()) {
			t.Errorf("%v 的默认并发不是10", args)
		}
	}
}

func TestHelpDoesNotReadCredentialsOrCallCloud(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"deploy", "-h"}, {"list", "-h"}, {"delete", "-h"}, {"check", "-h"}, {"bench", "-h"}, {"concurrency", "-h"}, {"gen-token", "-h"}} {
		var out bytes.Buffer
		if err := run(context.Background(), args, strings.NewReader(""), &out, false); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if out.Len() == 0 {
			t.Fatalf("%v 未输出帮助", args)
		}
	}
}
func TestGenTokenAndConfirmation(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"gen-token"}, strings.NewReader(""), &out, false); err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSpace(out.String())
	if len(token) != 64 {
		t.Fatalf("token长度=%d", len(token))
	}
	if err := confirm(strings.NewReader("yes\n"), &out, false, false); err == nil {
		t.Fatal("非交互必须显式yes flag")
	}
	if err := confirm(strings.NewReader("no\n"), &out, true, false); err == nil {
		t.Fatal("未确认不能继续")
	}
	if err := confirm(strings.NewReader("yes\n"), &out, true, false); err != nil {
		t.Fatal(err)
	}
}
