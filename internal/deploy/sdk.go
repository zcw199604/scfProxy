package deploy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	tcerr "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	scf "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/scf/v20180416"
)

// SCFAPI 是部署管理需要的最小 SDK 接口，测试使用本地假实现。
type SCFAPI interface {
	UpdateFunctionConfigurationWithContext(context.Context, *scf.UpdateFunctionConfigurationRequest) (*scf.UpdateFunctionConfigurationResponse, error)
	UpdateFunctionCodeWithContext(context.Context, *scf.UpdateFunctionCodeRequest) (*scf.UpdateFunctionCodeResponse, error)
	CreateFunctionWithContext(context.Context, *scf.CreateFunctionRequest) (*scf.CreateFunctionResponse, error)
	GetFunctionWithContext(context.Context, *scf.GetFunctionRequest) (*scf.GetFunctionResponse, error)
	CreateTriggerWithContext(context.Context, *scf.CreateTriggerRequest) (*scf.CreateTriggerResponse, error)
	ListTriggersWithContext(context.Context, *scf.ListTriggersRequest) (*scf.ListTriggersResponse, error)
	ListFunctionsWithContext(context.Context, *scf.ListFunctionsRequest) (*scf.ListFunctionsResponse, error)
	DeleteFunctionWithContext(context.Context, *scf.DeleteFunctionRequest) (*scf.DeleteFunctionResponse, error)
	DeleteTriggerWithContext(context.Context, *scf.DeleteTriggerRequest) (*scf.DeleteTriggerResponse, error)
}

// Factory 为指定地域创建客户端；构造过程不发送任何 API 请求。
type Factory func(region string) (SCFAPI, error)

// NewSDKFactoryFromEnv 仅从环境变量读取凭证，禁用 SDK 请求调试输出。
func NewSDKFactoryFromEnv() (Factory, error) {
	id, key, token := os.Getenv("TENCENTCLOUD_SECRET_ID"), os.Getenv("TENCENTCLOUD_SECRET_KEY"), os.Getenv("TENCENTCLOUD_SESSION_TOKEN")
	if id == "" || key == "" {
		return nil, errors.New("请设置 TENCENTCLOUD_SECRET_ID 和 TENCENTCLOUD_SECRET_KEY 环境变量")
	}
	credential := common.NewTokenCredential(id, key, token)
	return func(region string) (SCFAPI, error) {
		cfg := profile.NewClientProfile()
		cfg.HttpProfile.ReqTimeout = 30
		cfg.Debug = false
		return scf.NewClient(credential, region, cfg)
	}, nil
}

// ErrorCode 提取可用于重试和幂等判断的 SDK 错误码，不解析错误展示文本。
func ErrorCode(err error) string {
	var sdkErr *tcerr.TencentCloudSDKError
	if errors.As(err, &sdkErr) {
		return sdkErr.GetCode()
	}
	return ""
}

// SafeError 避免把 SDK 的服务端消息及其中可能回显的凭证写入输出。
// 只用于最终展示，调用方应在此之前使用原始错误决定流程。
func SafeError(err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	var sdkErr *tcerr.TencentCloudSDKError
	if errors.As(err, &sdkErr) {
		message = fmt.Sprintf("腾讯云 API 错误 %s (RequestId: %s)", sdkErr.GetCode(), sdkErr.GetRequestId())
	}
	var secrets []string
	for _, name := range []string{"TENCENTCLOUD_SECRET_ID", "TENCENTCLOUD_SECRET_KEY", "TENCENTCLOUD_SESSION_TOKEN", "SCFPROXY_TOKEN"} {
		if value := os.Getenv(name); value != "" {
			secrets = append(secrets, value)
		}
	}
	// 先替换长值，避免一个秘密是另一个秘密前缀时留下其余部分。
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	for _, secret := range secrets {
		message = strings.ReplaceAll(message, secret, "[已隐藏]")
	}
	return errors.New(message)
}

var _ SCFAPI = (*scf.Client)(nil)
