# scfproxy

Go 的腾讯云 SCF WebSocket 出口隧道，第 1 阶段实现。模块为 `github.com/zcw199604/scfproxy`，要求 Go 1.23 或更新版本。运行时第三方依赖只有 `github.com/coder/websocket`；`gorilla/websocket` 仅用于兼容性测试。

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

签名原文为 UTF-8 `v|target|ts|nonce`，版本与 Unix 秒为十进制，无末尾换行。服务端校验版本、±300 秒时差、16 字节 nonce 和 HMAC（常量时间比较），使用有到期时间、最多 65536 项的进程内缓存防重放。成功回复文本 `{"ok":true}`；失败返回 `ok:false` 及 `auth_failed`、`bad_request`、`forbidden_target` 或 `dial_failed`。然后只允许二进制消息作为 TCP 字节流，正常关闭使用 WS 1000。

服务端先解析目标，**检查所有解析结果后直接拨号已检查的 IP**，拒绝混有禁用地址的 DNS 结果，避免二次解析造成 DNS rebinding。默认拒绝 unspecified、loopback、RFC1918、link-local（包括元数据地址）、100.64.0.0/10、组播、IPv6 ULA 以及相应 IPv4-mapped 地址。`AllowPrivate` 会放开这些限制，只用于本地可信测试。

## 构建与验证

```bash
make test           # go test -race -count=1 ./...
make lint           # go vet 与 gofmt 检查
make build-server   # linux/amd64、CGO_ENABLED=0、trimpath、-s -w
make package-server # dist/scf-server.zip
```

zip 根目录仅包含 `scf-server` 与可执行的 `scf_bootstrap`；启动脚本执行 `exec ./scf-server`。需要本机 `zip` 命令。不包含云服务创建、配置或部署命令。

所有测试仅使用动态本地端口：HTTP/HTTPS+HTTP2、SSE 分段读取、coder/gorilla WS、大于 2 MiB 的双向随机数据及实际 WS 消息长度、认证拒绝不重试、私网过滤、Bypass、失败出口冷却、预热复用/Ping/关闭、deadline、合并及 Close flush、签名与地址规则。

## 已知限制

- SCF 每条 WS 对应一个实例；每个新 TCP 流仍需独立 WS，预热会占用实例和费用。
- 平台单消息 256 KiB、约 128 KiB/s、10 QPS 限制应结合实际套餐确认；Ping 等控制帧也可能计入平台限制，建议留出余量。下行限制可独立设置。
- 平台空闲超时及函数最长执行时间仍可能断开连接。Ping 不能延长函数执行上限；业务方负责重连和流恢复，库不会自动重放已发送业务数据。
- nonce 缓存仅在一个服务端进程有效，无法提供跨 SCF 实例防重放保证。保持时钟同步、使用高熵 token。
- 未连接真实腾讯云；函数 URL 原生 WebSocket 透传、平台控制帧行为、冷启动与限额需要部署阶段实测。
- 没有本地 HTTP/SOCKS5 监听端口、HTTP 打包转发或腾讯云部署功能。
