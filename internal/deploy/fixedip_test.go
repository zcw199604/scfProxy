package deploy

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	tcerr "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	scf "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/scf/v20180416"
)

func TestFixedIPCreateRequestAndWait(t *testing.T) {
	calls := 0
	f := &fakeAPI{create: func(r *scf.CreateFunctionRequest) error {
		if r.PublicNetConfig == nil || value(r.PublicNetConfig.PublicNetStatus) != "ENABLE" || r.PublicNetConfig.EipConfig == nil || value(r.PublicNetConfig.EipConfig.EipStatus) != "ENABLE" {
			t.Fatal("未启用固定公网IP")
		}
		return nil
	}, get: func() (*scf.GetFunctionResponse, error) {
		calls++
		r := &scf.GetFunctionResponseParams{Status: ptr("Active")}
		if calls >= 3 {
			r.PublicNetConfig = &scf.PublicNetConfigOut{EipConfig: &scf.EipConfigOut{EipAddress: []*string{ptr("203.0.113.9")}}}
		}
		return &scf.GetFunctionResponse{Response: r}, nil
	}}
	o := options()
	o.FixedIP = true
	m := testManager(f)
	m.EgressIPTimeout = time.Second
	result, err := m.Deploy(context.Background(), o)
	if err != nil || result[0].Err != nil || result[0].EgressIP != "203.0.113.9" || calls != 3 {
		t.Fatalf("result=%+v calls=%d err=%v", result, calls, err)
	}
}
func TestWaitEgressIPFallbackAndTimeout(t *testing.T) {
	for _, tt := range []struct {
		name     string
		response *scf.GetFunctionResponseParams
		want     string
	}{
		{"new_over_old", &scf.GetFunctionResponseParams{PublicNetConfig: &scf.PublicNetConfigOut{EipConfig: &scf.EipConfigOut{EipAddress: []*string{ptr("203.0.113.1")}}}, EipConfig: &scf.EipOutConfig{Eips: []*string{ptr("203.0.113.2")}}}, "203.0.113.1"},
		{"legacy", &scf.GetFunctionResponseParams{EipConfig: &scf.EipOutConfig{Eips: []*string{ptr("203.0.113.2")}}}, "203.0.113.2"},
		{"invalid_new_fallback", &scf.GetFunctionResponseParams{PublicNetConfig: &scf.PublicNetConfigOut{EipConfig: &scf.EipConfigOut{EipAddress: []*string{ptr("")}}}, EipConfig: &scf.EipOutConfig{Eips: []*string{ptr("203.0.113.2")}}}, "203.0.113.2"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := testManager(&fakeAPI{get: func() (*scf.GetFunctionResponse, error) { return &scf.GetFunctionResponse{Response: tt.response}, nil }})
			got, err := m.WaitEgressIP(context.Background(), "ap-guangzhou", "scfproxy-a")
			if err != nil || got != tt.want {
				t.Fatalf("got=%s err=%v", got, err)
			}
		})
	}
	m := testManager(&fakeAPI{})
	m.EgressIPTimeout = 5 * time.Millisecond
	_, err := m.WaitEgressIP(context.Background(), "ap-guangzhou", "scfproxy-a")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("未超时: %v", err)
	}
}
func TestListReadsEgressIPWithoutWaiting(t *testing.T) {
	gets := 0
	f := &fakeAPI{list: func(*scf.ListFunctionsRequest) (*scf.ListFunctionsResponse, error) {
		return &scf.ListFunctionsResponse{Response: &scf.ListFunctionsResponseParams{Functions: []*scf.Function{{FunctionName: ptr("scfproxy-a"), Description: ptr("scfproxy")}}}}, nil
	}, get: func() (*scf.GetFunctionResponse, error) {
		gets++
		return &scf.GetFunctionResponse{Response: &scf.GetFunctionResponseParams{EipConfig: &scf.EipOutConfig{Eips: []*string{ptr("203.0.113.2")}}}}, nil
	}}
	list, err := testManager(f).List(context.Background(), []string{"ap-guangzhou"}, "scfproxy")
	if err != nil || len(list) != 1 || list[0].EgressIP != "203.0.113.2" || gets != 1 {
		t.Fatalf("list=%+v gets=%d err=%v", list, gets, err)
	}
}
func TestCreateExplicitFunctionName(t *testing.T) {
	f := &fakeAPI{create: func(r *scf.CreateFunctionRequest) error {
		if value(r.FunctionName) != "scfproxy-ap-guangzhou-12345" {
			t.Errorf("错误名称: %s", value(r.FunctionName))
		}
		return nil
	}}
	result := testManager(f).Create(context.Background(), options(), Endpoint{Region: "ap-guangzhou", Function: "scfproxy-ap-guangzhou-12345"})
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	result = testManager(f).Create(context.Background(), options(), Endpoint{Region: "ap-guangzhou", Function: "other"})
	if result.Err == nil {
		t.Fatal("应拒绝不属于prefix的名称")
	}
}
func TestDeleteAbsentFunctionIsSuccess(t *testing.T) {
	item := Result{Endpoint: Endpoint{Region: "ap-guangzhou", Function: "scfproxy-a"}, Description: "scfproxy"}
	for _, code := range []string{"ResourceNotFound.Function", "ResourceNotFound.FunctionName", "ResourceNotFound.Namespace"} {
		t.Run(code, func(t *testing.T) {
			f := &fakeAPI{get: func() (*scf.GetFunctionResponse, error) {
				return nil, tcerr.NewTencentCloudSDKError(code, "不存在", "")
			}}
			result := testManager(f).Delete(context.Background(), []Result{item}, "scfproxy")
			if (result[0].Err == nil) != strings.HasPrefix(code, "ResourceNotFound.Function") {
				t.Fatalf("result=%+v", result)
			}
		})
	}
	f := &fakeAPI{delete: func(*scf.DeleteFunctionRequest) error {
		return tcerr.NewTencentCloudSDKError("ResourceNotFound.Function", "不存在", "")
	}}
	if result := testManager(f).Delete(context.Background(), []Result{item}, "scfproxy"); result[0].Err != nil {
		t.Fatal(result[0].Err)
	}
}
