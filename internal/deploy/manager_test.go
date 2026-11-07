package deploy

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	tcerr "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	scf "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/scf/v20180416"
)

func ptr[T any](v T) *T { return &v }

type fakeAPI struct {
	update        func(*scf.UpdateFunctionCodeRequest) error
	create        func(*scf.CreateFunctionRequest) error
	get           func() (*scf.GetFunctionResponse, error)
	trigger       func(*scf.CreateTriggerRequest) (*scf.CreateTriggerResponse, error)
	triggers      func(*scf.ListTriggersRequest) (*scf.ListTriggersResponse, error)
	list          func(*scf.ListFunctionsRequest) (*scf.ListFunctionsResponse, error)
	delete        func(*scf.DeleteFunctionRequest) error
	deleteTrigger func(*scf.DeleteTriggerRequest) error
}

func (f *fakeAPI) CreateFunctionWithContext(_ context.Context, r *scf.CreateFunctionRequest) (*scf.CreateFunctionResponse, error) {
	if f.create != nil {
		return nil, f.create(r)
	}
	return &scf.CreateFunctionResponse{}, nil
}
func (f *fakeAPI) GetFunctionWithContext(_ context.Context, _ *scf.GetFunctionRequest) (*scf.GetFunctionResponse, error) {
	if f.get != nil {
		return f.get()
	}
	return &scf.GetFunctionResponse{Response: &scf.GetFunctionResponseParams{Status: ptr("Active"), Description: ptr("scfproxy")}}, nil
}
func (f *fakeAPI) CreateTriggerWithContext(_ context.Context, r *scf.CreateTriggerRequest) (*scf.CreateTriggerResponse, error) {
	if f.trigger != nil {
		return f.trigger(r)
	}
	return &scf.CreateTriggerResponse{Response: &scf.CreateTriggerResponseParams{TriggerInfo: &scf.Trigger{TriggerDesc: ptr(`{"NetConfig":{"ExtranetUrl":"https://example.test/path?q=1"}}`)}}}, nil
}
func (f *fakeAPI) ListTriggersWithContext(_ context.Context, r *scf.ListTriggersRequest) (*scf.ListTriggersResponse, error) {
	if f.triggers != nil {
		return f.triggers(r)
	}
	return &scf.ListTriggersResponse{Response: &scf.ListTriggersResponseParams{}}, nil
}
func (f *fakeAPI) ListFunctionsWithContext(_ context.Context, r *scf.ListFunctionsRequest) (*scf.ListFunctionsResponse, error) {
	if f.list != nil {
		return f.list(r)
	}
	return &scf.ListFunctionsResponse{Response: &scf.ListFunctionsResponseParams{}}, nil
}
func (f *fakeAPI) DeleteFunctionWithContext(_ context.Context, r *scf.DeleteFunctionRequest) (*scf.DeleteFunctionResponse, error) {
	if f.delete != nil {
		return nil, f.delete(r)
	}
	return &scf.DeleteFunctionResponse{}, nil
}
func (f *fakeAPI) DeleteTriggerWithContext(_ context.Context, r *scf.DeleteTriggerRequest) (*scf.DeleteTriggerResponse, error) {
	if f.deleteTrigger != nil {
		return nil, f.deleteTrigger(r)
	}
	return &scf.DeleteTriggerResponse{}, nil
}
func testManager(f *fakeAPI) *Manager {
	return &Manager{Factory: func(string) (SCFAPI, error) { return f, nil }, PollInterval: time.Millisecond, WaitTimeout: time.Second, TriggerRetryDelay: time.Millisecond}
}
func options() Options {
	return Options{Regions: []string{"ap-guangzhou"}, Count: 1, Prefix: "scfproxy", Runtime: "Go1", Token: "仅测试令牌", Package: []byte("测试代码包"), Timeout: 900, IdleTimeout: 600, Memory: 128, Concurrency: 2}
}

func TestDeployParametersPollingAndTriggerRetry(t *testing.T) {
	f := &fakeAPI{}
	polls, triggers := 0, 0
	f.create = func(r *scf.CreateFunctionRequest) error {
		if *r.Type != "HTTP" || *r.ProtocolType != "WS" || *r.Runtime != "Go1" || *r.Namespace != "default" || *r.Timeout != 900 || *r.ProtocolParams.WSParams.IdleTimeOut != 600 || *r.MemorySize != 128 || *r.Description != "scfproxy" {
			t.Errorf("创建参数不符")
		}
		if *r.Code.ZipFile != base64.StdEncoding.EncodeToString(options().Package) || *r.Environment.Variables[0].Value != options().Token || *r.Environment.Variables[0].Key != "SCFPROXY_TOKEN" {
			t.Error("代码或环境变量不符")
		}
		if *r.AutoCreateClsTopic != "FALSE" || *r.AutoDeployClsTopicIndex != "FALSE" || *r.Tags[0].Key != "managed-by" {
			t.Error("日志或标签不符")
		}
		return nil
	}
	f.get = func() (*scf.GetFunctionResponse, error) {
		polls++
		status := "Creating"
		if polls > 1 {
			status = "Active"
		}
		return &scf.GetFunctionResponse{Response: &scf.GetFunctionResponseParams{Status: ptr(status)}}, nil
	}
	f.trigger = func(r *scf.CreateTriggerRequest) (*scf.CreateTriggerResponse, error) {
		triggers++
		if *r.Type != "http" || *r.TriggerName != "func_url" || *r.Enable != "OPEN" || !strings.Contains(*r.TriggerDesc, `"EnableExtranet":true`) {
			t.Error("触发器参数不符")
		}
		if triggers == 1 {
			return nil, tcerr.NewTencentCloudSDKError("FailedOperation", "暂未就绪", "")
		}
		return (&fakeAPI{}).CreateTriggerWithContext(context.Background(), r)
	}
	result, err := testManager(f).Deploy(context.Background(), options())
	if err != nil || len(result) != 1 || result[0].Err != nil || result[0].URL != "wss://example.test/path?q=1" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if polls != 2 || triggers != 2 {
		t.Fatalf("polls=%d triggers=%d", polls, triggers)
	}
}
func TestExistingFunctionOnlyObtainsURL(t *testing.T) {
	f := &fakeAPI{create: func(*scf.CreateFunctionRequest) error {
		return tcerr.NewTencentCloudSDKError("ResourceInUse.Function", "已存在", "")
	}, update: func(*scf.UpdateFunctionCodeRequest) error { t.Error("未指定update-code不应更新"); return nil }}
	f.triggers = func(*scf.ListTriggersRequest) (*scf.ListTriggersResponse, error) {
		return &scf.ListTriggersResponse{Response: &scf.ListTriggersResponseParams{Triggers: []*scf.TriggerInfo{{TriggerName: ptr("func_url"), Type: ptr("http"), TriggerDesc: ptr(`{"NetConfig":{"ExtranetUrl":"http://example.test/existing"}}`)}}}}, nil
	}
	f.trigger = func(*scf.CreateTriggerRequest) (*scf.CreateTriggerResponse, error) {
		t.Error("已有触发器不应再创建")
		return nil, errors.New("不应调用")
	}
	results, err := testManager(f).Deploy(context.Background(), options())
	if err != nil || results[0].Err != nil || !results[0].Existed || results[0].URL != "ws://example.test/existing" {
		t.Fatalf("results=%+v err=%v", results, err)
	}
}
func TestFailedFunctionReportsStatusReasons(t *testing.T) {
	f := &fakeAPI{get: func() (*scf.GetFunctionResponse, error) {
		return &scf.GetFunctionResponse{Response: &scf.GetFunctionResponseParams{Status: ptr("CreateFailed"), StatusReasons: []*scf.StatusReason{{ErrorCode: ptr("InvalidRuntime"), ErrorMessage: ptr("运行时不可用")}}}}, nil
	}}
	results, _ := testManager(f).Deploy(context.Background(), options())
	if results[0].Err == nil || !strings.Contains(results[0].Err.Error(), "InvalidRuntime") {
		t.Fatalf("缺少失败原因: %+v", results)
	}
	o := options()
	o.Timeout = 599
	if _, err := Plan(o); err == nil {
		t.Fatal("执行超时小于空闲超时必须失败")
	}
}
func TestDeleteOnlyManagedFunctionsAndPagination(t *testing.T) {
	f := &fakeAPI{}
	deleted := []string{}
	f.list = func(r *scf.ListFunctionsRequest) (*scf.ListFunctionsResponse, error) {
		if *r.Offset == 0 {
			return &scf.ListFunctionsResponse{Response: &scf.ListFunctionsResponseParams{TotalCount: ptr(int64(3)), Functions: []*scf.Function{{FunctionName: ptr("scfproxy-a"), Description: ptr("scfproxy"), Status: ptr("Active")}, {FunctionName: ptr("scfproxy-b"), Description: ptr("别人")}}}}, nil
		}
		return &scf.ListFunctionsResponse{Response: &scf.ListFunctionsResponseParams{TotalCount: ptr(int64(3)), Functions: []*scf.Function{{FunctionName: ptr("other"), Description: ptr("scfproxy")}}}}, nil
	}
	f.delete = func(r *scf.DeleteFunctionRequest) error { deleted = append(deleted, *r.FunctionName); return nil }
	m := testManager(f)
	items, err := m.List(context.Background(), []string{"ap-guangzhou"}, "scfproxy")
	if err != nil {
		t.Fatal(err)
	}
	items = append(items, Result{Endpoint: Endpoint{Function: "other"}, Description: "scfproxy"})
	results := m.Delete(context.Background(), items, "scfproxy")
	if len(deleted) != 1 || deleted[0] != "scfproxy-a" || results[1].Err == nil {
		t.Fatalf("deleted=%v results=%+v", deleted, results)
	}
}

func TestWaitActiveTimeout(t *testing.T) {
	f := &fakeAPI{get: func() (*scf.GetFunctionResponse, error) {
		return &scf.GetFunctionResponse{Response: &scf.GetFunctionResponseParams{Status: ptr("Creating")}}, nil
	}}
	m := testManager(f)
	m.WaitTimeout = 10 * time.Millisecond
	results, err := m.Deploy(context.Background(), options())
	if err != nil || results[0].Err == nil || !errors.Is(results[0].Err, context.DeadlineExceeded) {
		t.Fatalf("results=%+v err=%v", results, err)
	}
}
func TestExistingFunctionCreatesMissingURL(t *testing.T) {
	created := 0
	f := &fakeAPI{create: func(*scf.CreateFunctionRequest) error {
		return tcerr.NewTencentCloudSDKError("ResourceInUse.Function", "已存在", "")
	}}
	f.trigger = func(r *scf.CreateTriggerRequest) (*scf.CreateTriggerResponse, error) {
		created++
		return (&fakeAPI{}).CreateTriggerWithContext(context.Background(), r)
	}
	results, err := testManager(f).Deploy(context.Background(), options())
	if err != nil || results[0].Err != nil || created != 1 {
		t.Fatalf("created=%d results=%+v err=%v", created, results, err)
	}
}
func TestDeleteTriggerFallbackAndOwnershipRecheck(t *testing.T) {
	f := &fakeAPI{}
	deleted, unbound := 0, 0
	f.delete = func(*scf.DeleteFunctionRequest) error {
		deleted++
		if deleted == 1 {
			return tcerr.NewTencentCloudSDKError("ResourceInUse.Trigger", "先删除触发器", "")
		}
		return nil
	}
	f.triggers = func(*scf.ListTriggersRequest) (*scf.ListTriggersResponse, error) {
		return &scf.ListTriggersResponse{Response: &scf.ListTriggersResponseParams{TotalCount: ptr(uint64(1)), Triggers: []*scf.TriggerInfo{{TriggerName: ptr("func_url"), Type: ptr("http"), Qualifier: ptr("$LATEST")}}}}, nil
	}
	f.deleteTrigger = func(r *scf.DeleteTriggerRequest) error {
		unbound++
		if *r.TriggerName != "func_url" || *r.Type != "http" {
			t.Error("删除触发器参数不符")
		}
		return nil
	}
	m := testManager(f)
	item := Result{Endpoint: Endpoint{Region: "ap-guangzhou", Function: "scfproxy-a"}, Description: "scfproxy"}
	results := m.Delete(context.Background(), []Result{item}, "scfproxy")
	if results[0].Err != nil || deleted != 2 || unbound != 1 {
		t.Fatalf("results=%+v deleted=%d unbound=%d", results, deleted, unbound)
	}
	f.get = func() (*scf.GetFunctionResponse, error) {
		return &scf.GetFunctionResponse{Response: &scf.GetFunctionResponseParams{Description: ptr("别人的函数")}}, nil
	}
	results = m.Delete(context.Background(), []Result{item}, "scfproxy")
	if results[0].Err == nil || deleted != 2 {
		t.Fatal("删除前描述已变化时仍删除")
	}
}
func TestDeployPartialFailureDoesNotStopOthers(t *testing.T) {
	f := &fakeAPI{create: func(r *scf.CreateFunctionRequest) error {
		if strings.HasSuffix(*r.FunctionName, "-1") {
			return errors.New("创建失败")
		}
		return nil
	}}
	o := options()
	o.Count = 3
	results, err := testManager(f).Deploy(context.Background(), o)
	if err != nil || len(results) != 3 || results[0].Err == nil || results[1].Err != nil || results[2].Err != nil {
		t.Fatalf("results=%+v err=%v", results, err)
	}
}

func (f *fakeAPI) UpdateFunctionCodeWithContext(_ context.Context, r *scf.UpdateFunctionCodeRequest) (*scf.UpdateFunctionCodeResponse, error) {
	if f.update != nil {
		return nil, f.update(r)
	}
	return &scf.UpdateFunctionCodeResponse{}, nil
}
func TestUpdateExistingCodeRequiresOwnership(t *testing.T) {
	for _, owned := range []bool{true, false} {
		t.Run(fmt.Sprint(owned), func(t *testing.T) {
			updated := 0
			f := &fakeAPI{create: func(*scf.CreateFunctionRequest) error {
				return tcerr.NewTencentCloudSDKError("ResourceInUse.Function", "已存在", "")
			}}
			f.get = func() (*scf.GetFunctionResponse, error) {
				description := "other"
				if owned {
					description = "scfproxy"
				}
				return &scf.GetFunctionResponse{Response: &scf.GetFunctionResponseParams{Description: ptr(description), Status: ptr("Active"), Handler: ptr("scf-server"), Runtime: ptr("Go1")}}, nil
			}
			f.update = func(r *scf.UpdateFunctionCodeRequest) error {
				updated++
				if value(r.ZipFile) != base64.StdEncoding.EncodeToString(options().Package) || value(r.Handler) != "scf-server" || value(r.Namespace) != "default" {
					t.Error("更新代码参数不符")
				}
				return nil
			}
			f.triggers = func(*scf.ListTriggersRequest) (*scf.ListTriggersResponse, error) {
				return &scf.ListTriggersResponse{Response: &scf.ListTriggersResponseParams{Triggers: []*scf.TriggerInfo{{TriggerName: ptr("func_url"), Type: ptr("http"), TriggerDesc: ptr(`{"NetConfig":{"ExtranetUrl":"https://example.test/existing"}}`)}}}}, nil
			}
			o := options()
			o.UpdateCode = true
			results, err := testManager(f).Deploy(context.Background(), o)
			if err != nil {
				t.Fatal(err)
			}
			if owned {
				if updated != 1 || results[0].Err != nil || !results[0].Updated || results[0].URL != "wss://example.test/existing" {
					t.Fatalf("updated=%d results=%+v", updated, results)
				}
			} else if updated != 0 || results[0].Err == nil {
				t.Fatal("不属于本工具的函数被更新")
			}
		})
	}
}

const realTriggerDesc = `{"AuthType":"NONE","NetConfig":{"EnableIntranet":false,"IntranetUrl":"","IntranetHTTPUrl":"","ExtranetUrl":"https://fallback.ap-guangzhou.tencentscf.com/path?q=1","ExtranetHTTPUrl":"http://fallback.ap-guangzhou.tencentscf.com","WssIntranetUrl":"","WsIntranetUrl":"","WssExtranetUrl":"wss://preferred.ap-guangzhou.tencentscf.com/path?q=1","WsExtranetUrl":"ws://preferred.ap-guangzhou.tencentscf.com","EnableExtranet":true},"CorsConfig":{},"ApiGwCompatible":false,"EnableSimpleMode":false}`

func TestPlatformRenamedURLTriggerAcrossFlows(t *testing.T) {
	for _, flow := range []string{"deploy", "list", "update-code", "resource-in-use"} {
		t.Run(flow, func(t *testing.T) {
			calls := 0
			f := &fakeAPI{}
			f.triggers = func(*scf.ListTriggersRequest) (*scf.ListTriggersResponse, error) {
				calls++
				if flow == "resource-in-use" && calls == 1 {
					return &scf.ListTriggersResponse{Response: &scf.ListTriggersResponseParams{}}, nil
				}
				return &scf.ListTriggersResponse{Response: &scf.ListTriggersResponseParams{Triggers: []*scf.TriggerInfo{
					{TriggerName: ptr("not-a-url"), Type: ptr("http"), Qualifier: ptr("$LATEST"), TriggerDesc: ptr(`{"NetConfig":{"IntranetUrl":"http://internal"}}`)},
					{TriggerName: ptr("old-url-id"), Type: ptr("http"), Qualifier: ptr("1"), TriggerDesc: ptr(`{"NetConfig":{"ExtranetUrl":"https://old.example.test"}}`)},
					{TriggerName: ptr("clx1soje0n"), Type: ptr("http"), Qualifier: ptr("$LATEST"), TriggerDesc: ptr(realTriggerDesc)},
				}}}, nil
			}
			f.trigger = func(*scf.CreateTriggerRequest) (*scf.CreateTriggerResponse, error) {
				if flow != "resource-in-use" {
					t.Error("已有URL不应创建触发器")
				}
				return nil, tcerr.NewTencentCloudSDKError("ResourceInUse.TriggerName", "已存在", "")
			}
			f.list = func(*scf.ListFunctionsRequest) (*scf.ListFunctionsResponse, error) {
				return &scf.ListFunctionsResponse{Response: &scf.ListFunctionsResponseParams{Functions: []*scf.Function{{FunctionName: ptr("scfproxy-a"), Description: ptr("scfproxy")}}}}, nil
			}
			o := options()
			if flow == "update-code" {
				o.UpdateCode = true
				f.create = func(*scf.CreateFunctionRequest) error {
					return tcerr.NewTencentCloudSDKError("ResourceInUse.Function", "已存在", "")
				}
			}
			m := testManager(f)
			var results []Result
			var err error
			if flow == "list" {
				results, err = m.List(context.Background(), o.Regions, o.Prefix)
			} else {
				results, err = m.Deploy(context.Background(), o)
			}
			if err != nil || len(results) != 1 || results[0].Err != nil || results[0].URL != "wss://preferred.ap-guangzhou.tencentscf.com/path?q=1" {
				t.Fatalf("results=%+v err=%v", results, err)
			}
		})
	}
}
func TestEndpointURLFallsBackWhenWssMissing(t *testing.T) {
	got, err := endpointURL(`{"NetConfig":{"WssExtranetUrl":"","ExtranetUrl":"https://example.test/path?q=1"}}`)
	if err != nil || got != "wss://example.test/path?q=1" {
		t.Fatalf("url=%s err=%v", got, err)
	}
}
