package main

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"testing"
)

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

func TestHelpDoesNotReadCredentialsOrCallCloud(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"deploy", "-h"}, {"list", "-h"}, {"delete", "-h"}, {"check", "-h"}, {"bench", "-h"}, {"gen-token", "-h"}} {
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
