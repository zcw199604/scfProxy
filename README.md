# scfproxy

Go 的腾讯云 SCF WebSocket 出口隧道，包含核心库、云函数服务端及第 2 阶段部署管理 CLI。模块为 `github.com/zcw199604/scfproxy`，要求 Go 1.23 或更新版本。隧道运行时第三方依赖只有 `github.com/coder/websocket`，部署 CLI 另使用腾讯云 common/scf SDK；`gorilla/websocket` 仅用于兼容性测试。

## 架构

```text
Go 业务程序 → scfproxy.DialContext → ws/wss 函数 URL → scf-server → 目标 TCP
```

一条 WS 承载一条 TCP 连接，不多路复用，也不做 HTTP 请求打包。TLS 在业务客户端与目标之间协商，隧道传输原始字节，因此支持 HTTPS、TLS ALPN HTTP/2、SSE 和 WebSocket。函数进程监听 `0.0.0.0:9000`，endpoint 的路径和查询参数原样用于建连；认证完全在 WS 内进行，不依赖平台转发认证头。目标地址、DNS 和 TCP 拨号发生在服务端。

## 接入

```go
import (
    "net/http"
    "os"

    "github.com/zcw199604/scfproxy/pkg/scfproxy"
    "github.com/gorilla/websocket" // 此处为业务项目自行选择的客户端
)

p, err := scfproxy.New(scfproxy.Config{
    EndpointsFile: "endpoints.txt",
    Token:         os.Getenv("SCFPROXY_TOKEN"),
    Strategy:      scfproxy.StickyByHost, // 或 scfproxy.Random（默认）
    Bypass:        []string{"localhost", "*.internal", "10.0.0.0/8"},
})
if err != nil {
    panic(err)
}
defer p.Close()

transport := p.Transport()
defer transport.CloseIdleConnections()
httpClient := &http.Client{Transport: transport}
gorillaDialer := websocket.Dialer{NetDialContext: p.DialContext}
_ = httpClient
_ = gorillaDialer
// coder/websocket: websocket.Dial(ctx, url,
//     &websocket.DialOptions{HTTPClient: httpClient})
```

`endpoints.txt` 每行一个 `wss://` 或 `ws://` 地址；忽略空行和去掉首尾空白后以 `#` 开头的行。与 `Endpoints` 合并，顺序保留。线上应使用 `wss://`，本地测试使用 `ws://`。

`DialContext(ctx, network, addr)` 接受 `tcp`、`tcp4`、`tcp6` 和 `host:port`。代理模式的协议仅包含目标地址，最终 IP 地址族由服务端 DNS 结果决定；需要限定地址族时请传入相应 IP。Bypass 使用普通 `net.Dialer`，遵从传入的网络类型。返回的连接支持并发 Read/Write/Close、读写 deadline；拨号 ctx 仅控制建连与握手，不控制已交付连接的寿命。`Transport()` 克隆 `http.DefaultTransport`，关闭环境 HTTP 代理，启用 `ForceAttemptHTTP2`，无需额外设置 TLS 跳过验证。

## 客户端配置

| 字段 | 默认 / 说明 |
| --- | --- |
| `Endpoints`、`EndpointsFile` | 合并后必须至少一个有效 ws/wss URL |
| `Token` | 必填，与服务端一致 |
| `Strategy` | `Random`；`StickyByHost` 使用小写目标 host 的 FNV-1a 哈希定位，冷却时向后顺延 |
| `Bypass` | 精确主机名（忽略大小写与末尾点）、`*.suffix`（仅子域名）、CIDR、IP；不为域名的 CIDR 匹配额外解析 DNS |
| `DialTimeout` | 15s，WS 建连和握手整体预算，重试共享预算 |
| `MaxRetries` | 2，最多换两个 endpoint；失效预热连接额外在同一出口重建一次，不计入此次数 |
| `EndpointCooldown` | 30s；出口建连/握手传输失败及内部 `DialTimeout` 到期进入冷却；调用方取消/截止或 Proxy 关闭不冷却。优先健康出口，再按冷却到期时间从早到晚尝试其余出口，仍受 `MaxRetries` 约束；拨号成功后清除冷却 |
| `MaxMessageSize` | 64 KiB，上行单条二进制消息上限，最大 256 KiB |
| `MaxMessagesPerSecond` | 10，上行消息速率，容量为 1 的令牌桶 |
| `MaxBytesPerSecond` | 128 KiB/s，上行字节速率；与消息速率共同限制，允许首条消息的有限突发 |
| `DisableRateLimit` | false；true 同时关闭上行消息和字节限速 |
| `CoalesceDelay` | 2ms，从首个小包起计时；缓冲满立即发送 |
| `PingInterval` | 30s；负值关闭 Ping |
| `Prewarm` | 0，关闭预热；大于 0 时后台维持指定总数的未握手连接，轮流分配到健康 endpoint |
| `PrewarmMaxIdle` | 4 分钟，从 WS 建立开始计时；过期关闭重建 |
| `Logger` | nil，静默；可传入 `*slog.Logger` |

**零值约定：**上述有非零默认值的字段，0 表示使用默认值。原需求同时要求 `MaxMessagesPerSecond=0` 不限、`PingInterval=0` 关闭和非零默认值，普通 Go 配置字段无法区分“未填写”和“显式赋 0”；因此使用 `DisableRateLimit=true` 和 `PingInterval<0` 显式关闭。`Token` 与 endpoint 无法省略。负数重试次数、负数延时、无效消息大小等会返回配置错误。

Write 先把数据送入有界缓冲，上行泵按合并延时、分片大小、消息速率和字节速率发送；等待限速令牌期间继续合并小包，缓冲满后才阻塞形成背压；写入受到背压和 WriteDeadline 约束。Write 成功表示数据已被隧道接受，不表示对端已确认。空闲的小包也会自动发出。Close 尝试 flush 已接受的数据，最多等待 `DialTimeout`，然后关闭双方；网络错误或平台强制断开时无法保证送达。不实现 TCP 半关闭。

WS 连接及首条握手响应各由单个常驻读取器消费，预热时也持续处理 Ping/Pong。取出预热连接先停止其保活任务、等待正在进行的 Ping 完成，再发送目标握手；取出后异步补池。Random 优先复用池中任意健康 endpoint 的预热连接；Sticky 仅取其当前选择 endpoint 的连接，无对应预热连接时直接新建。Proxy.Close 关闭池和活动隧道并等待后台任务结束。重复 Close 安全。

仅 WS 建连和握手传输/格式错误换 endpoint 重试。服务端明确返回失败时立即返回 `*scfproxy.HandshakeError{Code, Message}`，可用 `errors.As` 判断；不会重放已开始的业务 TCP 流量。

## 服务端及协议

本地启动（`-allow-private` 仅用于访问本机测试目标）：

```bash
export SCFPROXY_TOKEN='请自行设置足够随机的共享令牌'
go run ./cmd/scf-server -addr 127.0.0.1:9000 -allow-private
```

Token 为空时拒绝启动。普通 HTTP 请求（包括 `/healthz`）返回 `200 ok`。使用 `log/slog` 记录连接、目标、结束原因及双向字节数，不记录 token 或签名。SIGTERM/SIGINT 会停止隧道并执行 HTTP Shutdown。

| 参数 | 默认 |
| --- | --- |
| `-addr` | `0.0.0.0:$PORT`，PORT 默认 9000 |
| `-allow-private` | false |
| `-handshake-timeout` | 10s，首消息开始后的完整读取超时，以及握手回复写入超时 |
| `-prewarm-timeout` | 5m，等待首条消息的最长时间 |
| `-dial-timeout` | 10s，DNS 查询和目标拨号共用预算 |
| `-max-message-size` | 65536，下行分片，最大 262144 |
| `-max-messages-per-second` | 0，下行不限消息数 |
| `-max-bytes-per-second` | 0，下行不限字节数 |

严格的“建连后 10 秒内必须收到握手”与“目标尚未确定的 4 分钟预热池”互相冲突。本实现用 `PrewarmTimeout` 控制首消息等待，首消息头到达后再施加 `HandshakeTimeout`，并限制握手消息为 4096 字节。需要严格 10 秒认证窗口时设置 `-prewarm-timeout=10s`，同时禁用预热或缩短客户端 `PrewarmMaxIdle`。客户端 `PrewarmMaxIdle` 必须小于服务端 `prewarm-timeout`，默认分别为 4 分钟和 5 分钟。取出的预热连接发生握手传输错误时，立即在同一 endpoint 新建连接重试一次，不计入 `MaxRetries`，也不冷却该 endpoint；只有新建连接仍失败才进入冷却和换出口重试。

客户端第一条文本消息为：

```json
{"v":1,"target":"example.com:443","ts":1700000000,"nonce":"随机16字节的小写hex","sign":"HMAC-SHA256的小写hex"}
```

拨号先检查所有解析结果，再按 IPv4 优先、IPv6 其后的顺序直接拨 IP。每个 IP 分配 `min(剩余预算, max(剩余预算/剩余IP数, 2s))` 的超时，避免单个黑洞地址耗尽全部预算。失败回复与 slog 日志包含脱敏的 DNS 原因、已尝试 IP 及错误；禁止地址会说明解析 IP 和禁止类别，不包含 token 或签名。

签名原文为 UTF-8 `v|target|ts|nonce`，版本与 Unix 秒为十进制，无末尾换行。服务端校验版本、±300 秒时差、16 字节 nonce 和 HMAC（常量时间比较），使用有到期时间、最多 65536 项的进程内缓存防重放。成功回复文本 `{"ok":true}`；失败返回 `ok:false` 及 `auth_failed`、`bad_request`、`forbidden_target` 或 `dial_failed`。然后只允许二进制消息作为 TCP 字节流，正常关闭使用 WS 1000。

服务端先解析目标，**检查所有解析结果后直接拨号已检查的 IP**，拒绝混有禁用地址的 DNS 结果，避免二次解析造成 DNS rebinding。默认拒绝 unspecified、loopback、RFC1918、link-local（包括元数据地址）、100.64.0.0/10、组播、IPv6 ULA 以及相应 IPv4-mapped 地址。`AllowPrivate` 会放开这些限制，只用于本地可信测试。

## 构建与验证

```bash
make test           # go test -race -count=1 ./...
make lint           # go vet 与 gofmt 检查
make build-server   # linux/amd64、CGO_ENABLED=0、trimpath、-s -w
make package-server # dist/scf-server.zip
```

zip 根目录仅包含 `scf-server` 与可执行的 `scf_bootstrap`；启动脚本执行 `exec ./scf-server`。需要本机 `zip` 命令。云服务部署管理请使用下文的独立 CLI。

所有测试仅使用动态本地端口：HTTP/HTTPS+HTTP2、SSE 分段读取、coder/gorilla WS、大于 2 MiB 的双向随机数据及实际 WS 消息长度、认证拒绝不重试、私网过滤、Bypass、失败出口冷却、预热复用/Ping/关闭、deadline、合并及 Close flush、签名与地址规则。

## 已知限制

- SCF 每条 WS 对应一个实例；每个新 TCP 流仍需独立 WS，预热会占用实例和费用。
- 平台单消息 256 KiB、约 128 KiB/s、10 QPS 限制应结合实际套餐确认；Ping 等控制帧也可能计入平台限制，建议留出余量。下行限制可独立设置。
- 平台空闲超时及函数最长执行时间仍可能断开连接。Ping 不能延长函数执行上限；业务方负责重连和流恢复，库不会自动重放已发送业务数据。
- nonce 缓存仅在一个服务端进程有效，无法提供跨 SCF 实例防重放保证。保持时钟同步、使用高熵 token。
- 已有用户提供的广州地域实测，详见“实测结果”；其他地域、限额边界及长时间运行行为仍需验证。
- 没有本地 HTTP/SOCKS5 监听端口或 HTTP 打包转发。

## 第 2 阶段：部署和管理 CLI

```bash
make package-server
make build-cli
./dist/scfproxy -h
./dist/scfproxy deploy -h
```

`make deploy` 只提示打包及手动运行步骤，不自动调用腾讯云。CLI 的部署管理代码使用按地域创建的 `SCFAPI` 接口，测试全部使用假接口；SDK 客户端构造不会发送网络请求。仅当用户手动执行对应命令时才访问云或检测目标。

凭证只能通过环境变量提供，CLI 不保存凭证，也不输出 SDK 可能回显请求内容的错误消息：

```bash
export TENCENTCLOUD_SECRET_ID='你的SecretId'
export TENCENTCLOUD_SECRET_KEY='你的SecretKey'
# 使用临时凭证时再设置 TENCENTCLOUD_SESSION_TOKEN
# token 只保存在当前环境，客户端和云函数必须使用同一个值
export SCFPROXY_TOKEN="$(./dist/scfproxy gen-token)"
```

`gen-token` 输出 32 字节随机值的 64 字符 hex，不写文件。请自行通过安全渠道保存共享 token；不要把环境变量文件或 shell 历史中的真实密钥提交到仓库。腾讯云凭证只供部署管理使用，业务客户端仅需 `SCFPROXY_TOKEN` 与 endpoints 文件。

部署例子（**执行会创建计费云资源**）：

```bash
./dist/scfproxy deploy -r ap-guangzhou,ap-shanghai -n 1 -o endpoints.txt
./dist/scfproxy deploy -a foreign -n 1 --runtime CustomRuntime --yes
# 更新已有且属于本工具的函数代码
./dist/scfproxy deploy -r ap-guangzhou --update-code --yes
```

`-r` 显式指定地域，或 `-a domestic|foreign|all` 选择预设地域组，两者互斥。domestic 包括北京、成都、广州、上海、南京和香港；foreign 包括新加坡、曼谷、首尔、东京、法兰克福、弗吉尼亚及硅谷，all 合并二者。显式地域允许未来新增地域名，不限制为预设列表。

函数名为 `<prefix>-<region>-<i>`，索引从 1 开始。默认参数为：

| 参数 | 默认 / 说明 |
| --- | --- |
| `--prefix` | scfproxy；字母开头、字母数字或 `-`/`_`、结尾为字母数字；完整函数名 2–60 字符 |
| `-n` | 每地域 1 个函数 |
| `--package` | dist/scf-server.zip；检查根目录仅有两个非空可执行文件 |
| `--runtime` | Go1；创建时可改 CustomRuntime；代码更新沿用现有 Runtime |
| `--update-code` | false；更新已有且名称前缀、Description 均匹配的函数代码 |
| `--timeout` | 900 秒；必须不小于 idle-timeout |
| `--idle-timeout` | 600 秒；必须为正数，实际平台范围待确认 |
| `--memory` | 128 MB；实际可用规格由平台校验 |
| `--concurrency` | 8；最多同时进行的函数创建流程 |
| `--wait-timeout` | 3 分钟；每个函数等待 Active 的总超时 |
| `-o` | endpoints.txt；成功地址合并去重，附加地域/函数名注释，权限 0644 |
| `--yes` | 跳过交互确认；非交互环境必须显式提供 |

创建请求采用 `Namespace=default`、`Type=HTTP`、`ProtocolType=WS`、Description 为 `scfproxy` 和 `managed-by=scfproxy` 标签，关闭自动创建 CLS 主题和索引。等待状态 Active 后获取或创建 `func_url` HTTP 触发器，关闭平台 URL 认证、打开公网访问；隧道内仍必须通过 HMAC 认证。创建触发器遇到 FailedOperation 每隔 5 秒重试，最多 10 次。识别函数 URL 时不依赖 TriggerName（平台可能改写为 URL ID），选择带有效外网地址的 HTTP 触发器，多个时优先 `$LATEST`。优先读取 `WssExtranetUrl`，为空才将 `ExtranetUrl` 的 https/http 转为 wss/ws，保留路径和查询参数。

默认同名函数存在时不覆盖代码、运行时、规格或 token，只读取/补建函数 URL。加 `--update-code` 时，先校验名称以 `<prefix>-` 开头且 Description 精确为 `scfproxy`，再调用 UpdateFunctionCode 更新 zip，等待 Active 后读取现有 func_url。SDK 的 UpdateFunctionCode 没有 Runtime 字段，因此沿用现有 Runtime 和 Handler；此参数不更新环境变量、token、内存或超时。需更改这些配置时请单独管理或先 delete 再 deploy。多函数部分失败会输出逐项结果并返回非零退出码，已创建的函数不会自动回滚；请用 list 核查，按需删除。endpoints 文件只保存地址及注释，不含 token。文件合并采用同目录临时文件原子替换；不支持多个 CLI 进程同时编辑同一文件。

列表和删除：

```bash
./dist/scfproxy list -a all
./dist/scfproxy delete -r ap-guangzhou --prefix scfproxy -o endpoints.txt
# 自动化环境必须显式确认
./dist/scfproxy delete -a foreign --yes -o endpoints.txt
```

list 分页查询并且只展示名称以 `<prefix>-` 开头、Description **精确等于** `scfproxy` 的函数，尽量读取 URL；URL 查询失败会单独提示。delete 先展示同样的清单，确认后再次核对描述，仅删除匹配资源；平台要求时先解绑触发器。成功删除后清理 endpoints 文件中的相应地址及紧邻的自动生成注释；无法获取 URL 时会提示手工核查文件。

出口检测：

```bash
./dist/scfproxy check -f endpoints.txt --target https://api.ipify.org -c 4
```

每个 endpoint 使用独立单出口 Proxy，输出建连+带内握手耗时、从请求开始计的首字节耗时（含隧道/TLS）、HTTP 状态和响应正文前 100 字节。单项预算为 30 秒，失败逐项显示并最终返回非零退出码；不跟随重定向。建连时间包含冷启动影响，但也包含网络和目标 TCP 拨号，不能视为纯平台冷启动时间。

下载、上传和空闲测试：

```bash
./dist/scfproxy bench -f endpoints.txt --bytes 10MB
./dist/scfproxy bench -f endpoints.txt --bytes 10MiB --no-rate-limit
./dist/scfproxy bench -f endpoints.txt --hold 12m --bytes 1MB
# 本地或自有测试目标可覆盖默认URL
./dist/scfproxy bench --download https://example.com/download --upload https://example.com/upload
```

默认下载 `https://mirrors.aliyun.com/ubuntu/ls-lR.gz`（实测约 39MB），所有下载（含 hold 后请求）最多读取 `--bytes` 指定字节数便停止并关闭响应体；目标提前结束时按实际读取量统计。没有可靠的公共上传默认目标，必须显式指定 `--upload URL`，否则跳过上传并提示。`MB` 为十进制，`MiB` 为二进制。每项单独输出实际字节数、总耗时及 B/s，包含建连及响应等待。非 2xx 状态作为警告显示，不清零已读取字节；I/O 错误仍报错并保留已读取量。上传计数为 HTTP Transport 从请求体实际读取的字节数，不代表目标应用已消费或确认的字节数。不跳过证书验证。默认每项总预算为 5 分钟加 hold 时长。

`--no-rate-limit` 关闭客户端上行 QPS/字节限速，以便探测真实平台限制；服务端下行限制仍取决于打包程序配置。`--hold` 额外建立一条到 **download URL 目标**的 TCP 隧道，空闲等待指定时间后在同一隧道发 GET；HTTPS 在等待结束后才握手。空闲期间没有 TCP 业务数据，仅库的默认 30 秒 WS Ping。hold 耗时和速率包含空闲等待，不用于衡量传输峰值。如果目标自身提前关闭空闲 TCP，也会失败；此时需结合函数日志区分目标行为与平台空闲超时。keepalive 无法绕过函数最长执行时间。

费用与安全：随用随删，部署及预热连接会产生实例、执行时间和流量费用；关闭自动 CLS 创建不代表关闭所有费用。函数 URL 不要公开，使用高熵 token、最小权限凭证和临时凭证；`AuthType=NONE` 表示公网可以发起 WS，HMAC 认证失败前仍可能占用实例。服务端 `-prewarm-timeout` 是**未认证连接等待首消息**的上限，默认 5 分钟；客户端池寿命默认 4 分钟且必须更短。如需调整服务端启动参数，请修改打包使用的 `deploy/scf_bootstrap` 后重新打包。

### 实测结果

以下为用户在真实腾讯云 `ap-guangzhou`、单个函数上的实测反馈；本轮代码修改和自动化测试未连接云服务。

- `Runtime=Go1`、`Type=HTTP`、`ProtocolType=WS`、`IdleTimeOut=60`、`Timeout=900` 的 CreateFunction 一次成功；CreateTrigger（http/func_url）返回 https 地址，转换为 wss 后可直接连接。
- 冷启动加带内握手约 200–650ms，出口 IP 为腾讯云广州地址。
- 下行约 650KB/s–1MB/s，约 39MB 文件持续下载约 650KB/s。此结果说明所观察到的 128KB/s 限制不作用于下行，不能据此确定其他地域或套餐上限。
- 不发送 Ping 的连接在约 60.8 秒被平台关闭，状态 1011，reason 为 `Connection idle Timeout:60`；每 30 秒发送 WS Ping 的连接存活超过 150 秒，确认 **WS Ping 会重置平台空闲计时**。
- `mirrors.cloud.tencent.com` 等腾讯云自家域名在函数中解析为内网地址，被目标过滤正确拦截，返回 forbidden_target；请勿为访问这些镜像而放开私网过滤。
- 国内目标及 ifconfig.me 正常；api.ipify.org / api4.ipify.org（Cloudflare）快速返回 dial_failed；httpbin.org、1.1.1.1:443 约 10 秒后 dial_failed；github.com 客户端超时。原服务端仅回复“目标连接失败”，尚不能确定具体原因。新版会返回并记录脱敏后的 DNS、逐 IP 拨号错误，便于后续判断。
- 上行上限尚未测出：旧版 bench 遇到上传目标提前返回非 2xx 时显示 0 字节；新版保留实际请求体读取量并附加状态警告，需要重新测试。

### 仍待实测

- 其他地域、CustomRuntime 兼容性，IdleTimeOut 的完整范围（SDK 注释 1–1800 秒，部分资料 10–7200 秒）；当前只确认 60 秒有效。
- Timeout 的完整范围及与 IdleTimeOut 的关系；当前只确认执行超时 900 秒、空闲超时 60 秒组合有效。
- 上行实际速率限制、10 QPS 是否只限制上行，以及控制帧是否计入消息配额。
- 函数最长执行时间断开、其他地域的冷启动与连接表现，以及海外目标失败的具体 DNS/网络原因。

本次固定 SDK 模块为 common `v1.3.210`、scf `v1.3.101`（实现时模块代理返回的最新稳定版本）。`CreateTrigger.Response.TriggerInfo` 的 SDK 类型为 `Trigger`，`ListTriggers.Response.Triggers` 为 `TriggerInfo` 列表，分别读取其 `TriggerDesc`；其余要求的创建字段均可直接设置。
