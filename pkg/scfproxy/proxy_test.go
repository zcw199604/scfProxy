package scfproxy

import "testing"

func TestConfigValidation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("空配置必须失败")
	}
	if _, err := New(Config{Token: "x", Endpoints: []string{"http://localhost"}}); err == nil {
		t.Fatal("必须拒绝非 WS 地址")
	}
}
