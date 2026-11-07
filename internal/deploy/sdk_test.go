package deploy

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	tcerr "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	scf "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/scf/v20180416"
)

func TestSDKFactoryReadsOnlyEnvironment(t *testing.T) {
	t.Setenv("TENCENTCLOUD_SECRET_ID", "")
	t.Setenv("TENCENTCLOUD_SECRET_KEY", "test-secret-key")
	if _, err := NewSDKFactoryFromEnv(); err == nil || strings.Contains(err.Error(), "test-secret-key") {
		t.Fatalf("应安全拒绝缺失凭证: %v", err)
	}
	t.Setenv("TENCENTCLOUD_SECRET_ID", "test-secret-id")
	t.Setenv("TENCENTCLOUD_SECRET_KEY", "")
	if _, err := NewSDKFactoryFromEnv(); err == nil || strings.Contains(err.Error(), "test-secret-id") {
		t.Fatalf("应安全拒绝缺失密钥: %v", err)
	}
	t.Setenv("TENCENTCLOUD_SECRET_KEY", "test-secret-key")
	t.Setenv("TENCENTCLOUD_SESSION_TOKEN", "test-session-token")
	factory, err := NewSDKFactoryFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	// 构造 SDK 客户端不发起网络请求。
	api, err := factory("ap-guangzhou")
	if err != nil || api == nil {
		t.Fatalf("客户端构造失败: %v", err)
	}
	client := api.(*scf.Client)
	credential := client.GetCredential()
	if client.GetRegion() != "ap-guangzhou" || credential.GetSecretId() != "test-secret-id" || credential.GetSecretKey() != "test-secret-key" || credential.GetToken() != "test-session-token" {
		t.Fatal("环境凭证或地域未正确传入客户端")
	}
}

func TestSDKErrorCodeAndSafeDisplay(t *testing.T) {
	for _, name := range []string{"TENCENTCLOUD_SECRET_ID", "TENCENTCLOUD_SECRET_KEY", "TENCENTCLOUD_SESSION_TOKEN", "SCFPROXY_TOKEN"} {
		t.Setenv(name, name+"-sensitive-value")
	}
	cloudErr := tcerr.NewTencentCloudSDKError("FailedOperation.Test", "服务端回显 SCFPROXY_TOKEN-sensitive-value", "request-123")
	wrapped := fmt.Errorf("创建失败: %w", cloudErr)
	if got := ErrorCode(wrapped); got != "FailedOperation.Test" {
		t.Fatalf("code=%q", got)
	}
	safe := SafeError(wrapped)
	if safe == nil || !strings.Contains(safe.Error(), "FailedOperation.Test") || !strings.Contains(safe.Error(), "request-123") || strings.Contains(safe.Error(), "服务端回显") {
		t.Fatalf("SDK 错误展示不安全: %v", safe)
	}
	plain := SafeError(errors.New("失败 TENCENTCLOUD_SECRET_ID-sensitive-value TENCENTCLOUD_SECRET_KEY-sensitive-value TENCENTCLOUD_SESSION_TOKEN-sensitive-value SCFPROXY_TOKEN-sensitive-value"))
	if strings.Contains(plain.Error(), "sensitive-value") {
		t.Fatalf("普通错误泄漏敏感字段: %v", plain)
	}
	if SafeError(nil) != nil || ErrorCode(errors.New("普通错误")) != "" {
		t.Fatal("空错误或非 SDK 错误处理错误")
	}
}
