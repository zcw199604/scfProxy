// Package deploy 实现可替换云接口的部署管理与本地探测工具。
package deploy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	scf "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/scf/v20180416"
)

type Options struct {
	UpdateCode                   bool
	Regions                      []string
	Count                        int
	Prefix, Runtime, Token       string
	Package                      []byte
	Timeout, IdleTimeout, Memory int64
	Concurrency                  int
}

type Result struct {
	Endpoint
	Status, Description string
	Existed             bool
	Updated             bool
	Err                 error
	URLWarning          error
}

// Manager 的时间参数可调整；Factory 不会在计划阶段调用。
type Manager struct {
	Factory                                      Factory
	PollInterval, WaitTimeout, TriggerRetryDelay time.Duration
}

func value[T any](p *T) (v T) {
	if p != nil {
		return *p
	}
	return v
}
func ref[T any](v T) *T { return &v }

func Plan(o Options) ([]Endpoint, error) {
	if len(o.Regions) == 0 || o.Count < 1 {
		return nil, errors.New("至少指定一个地域，且数量必须大于0")
	}
	if o.Timeout < 1 || o.IdleTimeout < 1 || o.Timeout < o.IdleTimeout {
		return nil, errors.New("timeout 和 idle-timeout 必须大于0，且 timeout >= idle-timeout")
	}
	if o.Memory < 1 || o.Runtime == "" || o.Concurrency < 1 {
		return nil, errors.New("memory、runtime 或 concurrency 无效")
	}
	var out []Endpoint
	for _, region := range o.Regions {
		for i := 1; i <= o.Count; i++ {
			name, err := FunctionName(o.Prefix, region, i)
			if err != nil {
				return nil, err
			}
			out = append(out, Endpoint{Region: region, Function: name})
		}
	}
	return out, nil
}
func (m *Manager) Deploy(ctx context.Context, o Options) ([]Result, error) {
	plan, err := Plan(o)
	if err != nil {
		return nil, err
	}
	if o.Token == "" {
		return nil, errors.New("缺少 SCFPROXY_TOKEN，请先运行 scfproxy gen-token")
	}
	if len(o.Package) == 0 {
		return nil, errors.New("代码包为空，请先 make package-server")
	}
	results := make([]Result, len(plan))
	workers := min(o.Concurrency, len(plan))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for n := 0; n < workers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				results[i] = m.deployOne(ctx, o, plan[i])
			}
		}()
	}
	for i := range plan {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return results, nil
}
func (m *Manager) deployOne(ctx context.Context, o Options, e Endpoint) Result {
	result := Result{Endpoint: e, Description: "scfproxy"}
	api, err := m.Factory(e.Region)
	if err != nil {
		result.Err = err
		return result
	}
	r := scf.NewCreateFunctionRequest()
	r.FunctionName = ref(e.Function)
	r.Namespace = ref("default")
	r.Type = ref("HTTP")
	r.Runtime = ref(o.Runtime)
	r.Code = &scf.Code{ZipFile: ref(base64.StdEncoding.EncodeToString(o.Package))}
	r.CodeSource = ref("ZipFile")
	r.ProtocolType = ref("WS")
	r.ProtocolParams = &scf.ProtocolParams{WSParams: &scf.WSParams{IdleTimeOut: ref(uint64(o.IdleTimeout))}}
	r.Timeout = ref(o.Timeout)
	r.MemorySize = ref(o.Memory)
	r.Description = ref("scfproxy")
	r.Environment = &scf.Environment{Variables: []*scf.Variable{{Key: ref("SCFPROXY_TOKEN"), Value: ref(o.Token)}}}
	r.Tags = []*scf.Tag{{Key: ref("managed-by"), Value: ref("scfproxy")}}
	r.AutoDeployClsTopicIndex = ref("FALSE")
	r.AutoCreateClsTopic = ref("FALSE")
	_, err = api.CreateFunctionWithContext(ctx, r)
	if err != nil {
		if code := ErrorCode(err); code == "ResourceInUse.Function" || code == "ResourceInUse.FunctionName" || code == "ResourceInUse.FunctionAlreadyExists" {
			result.Existed = true
		} else {
			result.Err = err
			return result
		}
	}
	if result.Existed && o.UpdateCode {
		get := scf.NewGetFunctionRequest()
		get.FunctionName = ref(e.Function)
		get.Namespace = ref("default")
		current, getErr := api.GetFunctionWithContext(ctx, get)
		if getErr != nil {
			result.Err = getErr
			return result
		}
		if current == nil || current.Response == nil || !managed(e.Function, value(current.Response.Description), o.Prefix) {
			result.Err = errors.New("拒绝更新：函数前缀或Description不属于scfproxy")
			return result
		}
		update := scf.NewUpdateFunctionCodeRequest()
		update.FunctionName = ref(e.Function)
		update.Namespace = ref("default")
		update.ZipFile = r.Code.ZipFile
		update.CodeSource = ref("ZipFile")
		// UpdateFunctionCode 不提供 Runtime 字段，沿用原函数运行时和 Handler。
		update.Handler = current.Response.Handler
		if _, err = api.UpdateFunctionCodeWithContext(ctx, update); err != nil {
			result.Err = err
			return result
		}
		result.Updated = true
	}
	if err = m.waitActive(ctx, api, e.Function); err != nil {
		result.Err = err
		return result
	}
	result.Status = "Active"
	result.URL, result.Err = m.functionURL(ctx, api, e.Function, !result.Updated)
	return result
}
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *Manager) waitActive(ctx context.Context, api SCFAPI, name string) error {
	timeout := m.WaitTimeout
	if timeout <= 0 {
		timeout = 3 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	interval := m.PollInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	for {
		r := scf.NewGetFunctionRequest()
		r.FunctionName = ref(name)
		r.Namespace = ref("default")
		resp, err := api.GetFunctionWithContext(ctx, r)
		if err != nil {
			return err
		}
		if resp == nil || resp.Response == nil {
			return errors.New("GetFunction 返回空响应")
		}
		f := resp.Response
		status := value(f.Status)
		if status == "Active" {
			return nil
		}
		if strings.Contains(strings.ToLower(status), "fail") || status == "Deleted" || status == "Deleting" {
			var reasons []string
			for _, reason := range f.StatusReasons {
				if reason != nil {
					reasons = append(reasons, value(reason.ErrorCode)+": "+value(reason.ErrorMessage))
				}
			}
			return fmt.Errorf("函数状态 %s: %s", status, strings.Join(reasons, "; "))
		}
		if err = wait(ctx, interval); err != nil {
			return fmt.Errorf("等待函数 Active 超时或取消: %w", err)
		}
	}
}
func triggers(ctx context.Context, api SCFAPI, name string) ([]*scf.TriggerInfo, error) {
	var all []*scf.TriggerInfo
	for offset := uint64(0); ; {
		r := scf.NewListTriggersRequest()
		r.FunctionName = ref(name)
		r.Namespace = ref("default")
		r.Offset = ref(offset)
		r.Limit = ref(uint64(100))
		resp, err := api.ListTriggersWithContext(ctx, r)
		if err != nil {
			return nil, err
		}
		if resp == nil || resp.Response == nil {
			return nil, errors.New("ListTriggers 返回空响应")
		}
		page := resp.Response
		all = append(all, page.Triggers...)
		offset += uint64(len(page.Triggers))
		if len(page.Triggers) == 0 || offset >= value(page.TotalCount) {
			return all, nil
		}
	}
}
func endpointURL(desc string) (string, error) {
	var cfg struct {
		NetConfig struct {
			ExtranetURL    string `json:"ExtranetUrl"`
			WssExtranetURL string `json:"WssExtranetUrl"`
		} `json:"NetConfig"`
	}
	if err := json.Unmarshal([]byte(desc), &cfg); err != nil {
		return "", errors.New("触发器描述不是有效 JSON")
	}
	address := cfg.NetConfig.WssExtranetURL
	if address == "" {
		address = cfg.NetConfig.ExtranetURL
	}
	u, err := url.Parse(address)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" {
		return "", errors.New("触发器未返回有效外网地址")
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	case "wss", "ws":
	default:
		return "", errors.New("函数 URL 协议无效")
	}
	return u.String(), nil
}

// findFunctionURL 不依赖平台可能改写的 TriggerName，优先选择最新版本的外网 URL。
func findFunctionURL(list []*scf.TriggerInfo) (string, bool) {
	var fallback string
	for _, t := range list {
		if t == nil || value(t.Type) != "http" {
			continue
		}
		address, err := endpointURL(value(t.TriggerDesc))
		if err != nil {
			continue
		}
		if value(t.Qualifier) == "$LATEST" {
			return address, true
		}
		if fallback == "" {
			fallback = address
		}
	}
	return fallback, fallback != ""
}
func (m *Manager) functionURL(ctx context.Context, api SCFAPI, name string, create bool) (string, error) {
	list, err := triggers(ctx, api, name)
	if err != nil {
		return "", err
	}
	if address, ok := findFunctionURL(list); ok {
		return address, nil
	}
	if !create {
		return "", errors.New("未找到带有效外网地址的 HTTP 函数 URL 触发器")
	}
	delay := m.TriggerRetryDelay
	if delay <= 0 {
		delay = 5 * time.Second
	}
	for attempt := 0; attempt < 10; attempt++ {
		r := scf.NewCreateTriggerRequest()
		r.FunctionName = ref(name)
		r.Namespace = ref("default")
		r.Type = ref("http")
		r.TriggerName = ref("func_url")
		r.Enable = ref("OPEN")
		r.TriggerDesc = ref(`{"AuthType":"NONE","NetConfig":{"EnableIntranet":false,"EnableExtranet":true}}`)
		resp, err := api.CreateTriggerWithContext(ctx, r)
		if err == nil {
			if resp == nil || resp.Response == nil || resp.Response.TriggerInfo == nil {
				return "", errors.New("CreateTrigger 返回空响应")
			}
			return endpointURL(value(resp.Response.TriggerInfo.TriggerDesc))
		}
		code := ErrorCode(err)
		if strings.HasPrefix(code, "ResourceInUse") {
			list, e := triggers(ctx, api, name)
			if e != nil {
				return "", e
			}
			if address, ok := findFunctionURL(list); ok {
				return address, nil
			}
			return "", err
		}
		if !strings.HasPrefix(code, "FailedOperation") || attempt == 9 {
			return "", err
		}
		if err = wait(ctx, delay); err != nil {
			return "", err
		}
	}
	return "", errors.New("创建触发器失败")
}
func managed(name, description, prefix string) bool {
	return strings.HasPrefix(name, prefix+"-") && description == "scfproxy"
}
func (m *Manager) List(ctx context.Context, regions []string, prefix string) ([]Result, error) {
	if err := ValidatePrefix(prefix); err != nil {
		return nil, err
	}
	var results []Result
	for _, region := range regions {
		api, err := m.Factory(region)
		if err != nil {
			return nil, err
		}
		for offset := int64(0); ; {
			r := scf.NewListFunctionsRequest()
			r.Namespace = ref("default")
			r.SearchKey = ref(prefix + "-")
			r.Offset = ref(offset)
			r.Limit = ref(int64(100))
			resp, err := api.ListFunctionsWithContext(ctx, r)
			if err != nil {
				return nil, err
			}
			if resp == nil || resp.Response == nil {
				return nil, errors.New("ListFunctions 返回空响应")
			}
			page := resp.Response
			for _, f := range page.Functions {
				if f == nil || !managed(value(f.FunctionName), value(f.Description), prefix) {
					continue
				}
				item := Result{Endpoint: Endpoint{Region: region, Function: value(f.FunctionName)}, Description: value(f.Description), Status: value(f.Status)}
				item.URL, item.URLWarning = m.functionURL(ctx, api, item.Function, false)
				results = append(results, item)
			}
			offset += int64(len(page.Functions))
			if len(page.Functions) == 0 || offset >= value(page.TotalCount) {
				break
			}
		}
	}
	return results, nil
}
func (m *Manager) Delete(ctx context.Context, items []Result, prefix string) []Result {
	out := make([]Result, len(items))
	copy(out, items)
	for i := range out {
		item := &out[i]
		if err := ValidatePrefix(prefix); err != nil {
			item.Err = err
			continue
		}
		if !managed(item.Function, item.Description, prefix) {
			item.Err = errors.New("拒绝删除不匹配前缀和描述的函数")
			continue
		}
		api, err := m.Factory(item.Region)
		if err != nil {
			item.Err = err
			continue
		}
		// 删除前重新核对拥有者，防止仅凭调用方传入的列表删除其他函数。
		get := scf.NewGetFunctionRequest()
		get.FunctionName = ref(item.Function)
		get.Namespace = ref("default")
		current, err := api.GetFunctionWithContext(ctx, get)
		if err != nil {
			item.Err = err
			continue
		}
		if current == nil || current.Response == nil || !managed(item.Function, value(current.Response.Description), prefix) {
			item.Err = errors.New("删除前描述复核不匹配")
			continue
		}
		r := scf.NewDeleteFunctionRequest()
		r.FunctionName = ref(item.Function)
		r.Namespace = ref("default")
		_, err = api.DeleteFunctionWithContext(ctx, r)
		// 部分地域要求先解绑触发器；只在明确的资源占用/触发器错误下回退。
		code := ErrorCode(err)
		if err != nil && (strings.HasPrefix(code, "ResourceInUse") || strings.Contains(strings.ToLower(code), "trigger")) {
			list, e := triggers(ctx, api, item.Function)
			if e != nil {
				item.Err = e
				continue
			}
			for _, t := range list {
				if t == nil {
					continue
				}
				del := scf.NewDeleteTriggerRequest()
				del.FunctionName = ref(item.Function)
				del.Namespace = ref("default")
				del.TriggerName = t.TriggerName
				del.Type = t.Type
				del.Qualifier = t.Qualifier
				del.TriggerDesc = t.TriggerDesc
				if _, e = api.DeleteTriggerWithContext(ctx, del); e != nil {
					break
				}
			}
			if e != nil {
				item.Err = e
				continue
			}
			_, err = api.DeleteFunctionWithContext(ctx, r)
		}
		item.Err = err
	}
	return out
}
