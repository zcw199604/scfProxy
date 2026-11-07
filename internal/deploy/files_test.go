package deploy

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestResolveRegions(t *testing.T) {
	for _, tt := range []struct {
		regions, area string
		count         int
		bad           bool
	}{
		{"", "domestic", 6, false}, {"", "foreign", 7, false}, {"", "all", 13, false},
		{" ap-guangzhou,ap-shanghai,ap-guangzhou ", "", 2, false}, {"ap-future-1", "", 1, false},
		{"", "", 0, true}, {"ap-guangzhou", "all", 0, true}, {"", "unknown", 0, true}, {"ap-guangzhou,", "", 0, true}, {"../x", "", 0, true},
	} {
		got, err := ResolveRegions(tt.regions, tt.area)
		if (err != nil) != tt.bad {
			t.Fatalf("%q %q: %v", tt.regions, tt.area, err)
		}
		if err == nil && len(got) != tt.count {
			t.Fatalf("regions=%v", got)
		}
	}
}
func TestFunctionNameValidation(t *testing.T) {
	for _, tt := range []struct {
		prefix string
		bad    bool
	}{{"scfproxy", false}, {"x", false}, {"Alpha_1", false}, {"1bad", true}, {"bad-", true}, {"bad_", true}, {"a.b", true}, {"", true}, {strings.Repeat("a", 61), true}} {
		if err := ValidatePrefix(tt.prefix); (err != nil) != tt.bad {
			t.Fatalf("%q: %v", tt.prefix, err)
		}
	}
	name, err := FunctionName("scfproxy", "ap-guangzhou", 1)
	if err != nil || name != "scfproxy-ap-guangzhou-1" {
		t.Fatalf("%q %v", name, err)
	}
	if _, err := FunctionName(strings.Repeat("a", 59), "ap-guangzhou", 1); err == nil {
		t.Fatal("应拒绝过长函数名")
	}
	if _, err := FunctionName("scfproxy", "ap-guangzhou", 0); err == nil {
		t.Fatal("索引必须为正数")
	}
}
func TestEndpointsMergeDeduplicateAndRemove(t *testing.T) {
	path := filepath.Join(t.TempDir(), "endpoints.txt")
	original := "# 用户备注\n# ap-beijing existing\nwss://existing.example/path\n\n# 不应移除的其他注释\n"
	if err := os.WriteFile(path, []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	add := []Endpoint{{"ap-beijing", "existing", "wss://existing.example/path"}, {"ap-shanghai", "new", "wss://new.example/path"}, {"ap-shanghai", "new", "wss://new.example/path"}}
	if err := MergeEndpoints(path, add); err != nil {
		t.Fatal(err)
	}
	urls, err := ReadEndpoints(path)
	if err != nil || !reflect.DeepEqual(urls, []string{"wss://existing.example/path", "wss://new.example/path"}) {
		t.Fatalf("%v %v", urls, err)
	}
	data, _ := os.ReadFile(path)
	if strings.Count(string(data), "# ap-shanghai new") != 1 || !strings.HasPrefix(string(data), original) {
		t.Fatalf("注释未保留或重复: %s", data)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0644 {
		t.Fatalf("权限=%v", info.Mode())
	}
	if err := RemoveEndpoints(path, []string{"wss://existing.example/path", "wss://new.example/path"}); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(path)
	if strings.Contains(string(data), "# ap-beijing existing") || strings.Contains(string(data), "# ap-shanghai new") || !strings.Contains(string(data), "# 用户备注") || !strings.Contains(string(data), "# 不应移除的其他注释") {
		t.Fatalf("删除错误: %s", data)
	}
	if err := RemoveEndpoints(filepath.Join(t.TempDir(), "missing"), nil); err != nil {
		t.Fatal(err)
	}
}
func TestReadEndpointsValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "endpoints.txt")
	for _, tt := range []struct {
		data string
		bad  bool
		want []string
	}{
		{"# comment\n\nws://localhost:1234/path\n ws://localhost:1234/path \n", false, []string{"ws://localhost:1234/path"}},
		{"https://example.com\n", true, nil}, {"wss:///missing-host\n", true, nil}, {"wss://user:secret@example.com\n", true, nil},
	} {
		os.WriteFile(path, []byte(tt.data), 0644)
		got, err := ReadEndpoints(path)
		if (err != nil) != tt.bad || !reflect.DeepEqual(got, tt.want) {
			t.Fatalf("got=%v err=%v", got, err)
		}
	}
}
