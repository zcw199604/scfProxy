package deploy

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	tcerr "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	scf "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/scf/v20180416"
)

func TestCreateInstanceConcurrencyParameters(t *testing.T) {
	for _, n := range []int{0, 10, 100} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			o := options()
			o.MaxConcurrency = n
			f := &fakeAPI{create: func(r *scf.CreateFunctionRequest) error {
				c := r.InstanceConcurrencyConfig
				if n == 0 {
					if c != nil {
						t.Error("零并发应省略配置")
					}
				} else if c == nil || value(c.MaxConcurrency) != uint64(n) || value(c.DynamicEnabled) != "FALSE" || value(c.Type) != "Request-Based" {
					t.Errorf("并发配置错误: %+v", c)
				}
				return nil
			}}
			r, e := testManager(f).Deploy(context.Background(), o)
			if e != nil || r[0].Err != nil {
				t.Fatalf("results=%+v err=%v", r, e)
			}
		})
	}
}

func TestConcurrencyReportsFiveTunnelsOnOneInstance(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer target.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, err := Concurrency(ctx, ConcurrencyOptions{Endpoint: localProbeEndpoint(t), Token: "local-probe-token", Target: strings.TrimPrefix(target.URL, "http://"), Count: 5, Hold: 20 * time.Millisecond})
	if err != nil || r.Failures != 0 || len(r.Instances) != 1 || len(r.Tunnels) != 5 || r.MaxActive != 5 || r.P50 <= 0 || r.MaxDial < r.P50 {
		t.Fatalf("report=%+v err=%v", r, err)
	}
	for _, n := range r.Instances {
		if n != 5 {
			t.Fatalf("同实例隧道数=%d", n)
		}
	}
}

func TestConcurrencyHeadKeepsSameTCPConnection(t *testing.T) {
	var requests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("请求方法=%s", r.Method)
		}
		requests.Add(1)
		w.Header().Set("Content-Length", "100")
	}))
	defer target.Close()
	c, err := net.Dial("tcp", strings.TrimPrefix(target.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	reader := bufio.NewReader(c)
	for i := 0; i < 2; i++ {
		if err = concurrencyHead(c, reader, strings.TrimPrefix(target.URL, "http://")); err != nil {
			t.Fatal(err)
		}
	}
	if requests.Load() != 2 {
		t.Fatal(requests.Load())
	}
}

func TestConcurrencyValidationAndCancellation(t *testing.T) {
	for _, o := range []ConcurrencyOptions{{Count: 0}, {Count: 1, Hold: -1}, {Count: 1, Stagger: -1}, {Count: 1, Target: "https://bad"}} {
		if _, err := Concurrency(context.Background(), o); err == nil {
			t.Fatalf("接受无效参数: %+v", o)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := Concurrency(ctx, ConcurrencyOptions{Endpoint: localProbeEndpoint(t), Token: "local-probe-token", Count: 5, Target: "127.0.0.1:1", Hold: time.Minute})
	if err == nil || r.Failures != 5 {
		t.Fatalf("取消未生效: %+v %v", r, err)
	}
}

func TestListReadsInstanceConcurrency(t *testing.T) {
	f := &fakeAPI{}
	f.list = func(*scf.ListFunctionsRequest) (*scf.ListFunctionsResponse, error) {
		return &scf.ListFunctionsResponse{Response: &scf.ListFunctionsResponseParams{Functions: []*scf.Function{{FunctionName: ptr("scfproxy-ap-guangzhou-1"), Description: ptr("scfproxy")}}}}, nil
	}
	f.get = func() (*scf.GetFunctionResponse, error) {
		return &scf.GetFunctionResponse{Response: &scf.GetFunctionResponseParams{InstanceConcurrencyConfig: instanceConcurrency(10)}}, nil
	}
	r, err := testManager(f).List(context.Background(), []string{"ap-guangzhou"}, "scfproxy")
	if err != nil || len(r) != 1 || r[0].MaxConcurrency == nil || *r[0].MaxConcurrency != 10 {
		t.Fatalf("results=%+v err=%v", r, err)
	}
}

func TestUpdateConfigOwnershipCancellationAndCombinedCode(t *testing.T) {
	for _, tc := range []struct {
		n           int
		owned, code bool
	}{{10, true, false}, {0, true, false}, {10, false, false}, {10, true, true}} {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			configCalls, codeCalls := 0, 0
			pending, activeAfterUpdate := 0, false
			f := &fakeAPI{create: func(*scf.CreateFunctionRequest) error {
				return tcerr.NewTencentCloudSDKError("ResourceInUse.Function", "exists", "")
			}}
			f.get = func() (*scf.GetFunctionResponse, error) {
				desc := "other"
				if tc.owned {
					desc = "scfproxy"
				}
				status := "Active"
				if pending > 0 {
					status = "Updating"
					pending--
				} else if codeCalls > 0 || configCalls > 0 {
					activeAfterUpdate = true
				}
				return &scf.GetFunctionResponse{Response: &scf.GetFunctionResponseParams{Description: ptr(desc), Status: ptr(status)}}, nil
			}
			f.update = func(*scf.UpdateFunctionCodeRequest) error {
				codeCalls++
				pending = 1
				activeAfterUpdate = false
				return nil
			}
			f.triggers = func(*scf.ListTriggersRequest) (*scf.ListTriggersResponse, error) {
				return &scf.ListTriggersResponse{Response: &scf.ListTriggersResponseParams{Triggers: []*scf.TriggerInfo{{Type: ptr("http"), TriggerDesc: ptr(realTriggerDesc)}}}}, nil
			}
			f.updateConfig = func(r *scf.UpdateFunctionConfigurationRequest) error {
				if tc.code && !activeAfterUpdate {
					t.Error("代码更新后未等Active就更新配置")
				}
				configCalls++
				pending = 1
				activeAfterUpdate = false
				c := r.InstanceConcurrencyConfig
				if value(r.MemorySize) != 64 || value(r.Namespace) != "default" || value(r.FunctionName) != "scfproxy-ap-guangzhou-1" || c == nil {
					t.Errorf("配置更新参数错误: %+v", r)
					return fmt.Errorf("配置更新参数错误")
				}
				if tc.n == 0 {
					if value(c.DynamicEnabled) != "" || c.DynamicEnabled == nil || c.MaxConcurrency != nil {
						t.Error("零并发应明确取消配置")
					}
				} else if value(c.DynamicEnabled) != "FALSE" || value(c.MaxConcurrency) != uint64(tc.n) || value(c.Type) != "Request-Based" {
					t.Errorf("配置错误:%+v", c)
				}
				return nil
			}
			o := options()
			o.Memory = 64
			o.UpdateCode = tc.code
			o.UpdateConfig = true
			o.MaxConcurrency = tc.n
			r, e := testManager(f).Deploy(context.Background(), o)
			if e != nil {
				t.Fatal(e)
			}
			if tc.owned {
				if configCalls != 1 || r[0].Err != nil || !r[0].ConfigUpdated || !activeAfterUpdate {
					t.Fatalf("未更新: calls=%d results=%+v", configCalls, r)
				}
			} else if configCalls != 0 || r[0].Err == nil {
				t.Fatalf("归属校验失败: calls=%d results=%+v", configCalls, r)
			}
			if tc.code && codeCalls != 1 {
				t.Fatal("未同时更新代码")
			}
		})
	}
}

func TestMaxConcurrencyValidation(t *testing.T) {
	for _, n := range []int{-1, 101} {
		o := options()
		o.MaxConcurrency = n
		if _, err := Plan(o); err == nil {
			t.Errorf("接受无效并发数%d", n)
		}
	}
}
