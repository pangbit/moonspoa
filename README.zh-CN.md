# pangbit/moonspoa

[English](README.mbt.md) | 简体中文

MoonBit 实现的 SPOA（Stream Processing Offload Agent）库：HAProxy SPOE v1.2 的 SPOP 协议编解码、agent 服务器（TCP / Unix domain socket）与 SPOE 客户端。目标后端：**native**。仓库托管于 <https://github.com/pangbit/moonspoa>。

## 包结构与依赖层级

依赖方向严格单向，各子包可独立引用：

| 包 | 用途 | 依赖 |
| --- | --- | --- |
| `pangbit/moonspoa/spop` | 协议编解码与 HELLO 协商 | 仅 MoonBit core；后端无关 |
| `pangbit/moonspoa/agent` | Agent 配置与会话状态机 | `spop`、async 运行时及 IO；传输由调用方提供 |
| `pangbit/moonspoa/client` | HELLO、顺序 / pipelined NOTIFY、超时、DISCONNECT | `spop`、async 运行时、IO 与队列 |
| `pangbit/moonspoa/server` | TCP 与 Unix domain socket 传输 | `agent`、async 网络库与 C stub；仅 native |
| `pangbit/moonspoa` | 便捷入口 | 重导出 `spop` 类型及 `agent` 的 `Agent` / `Session`；**不包含** `server` 和 `client` |

- 只要编解码：引 `pangbit/moonspoa/spop`；
- 自己接网络：引 `pangbit/moonspoa/agent`（或 `client`）；
- 开箱即用的 TCP/UDS 服务器：引 `pangbit/moonspoa/server`（**需单独 import**，根包不重导出它，因为它是 native-only 且依赖较重）；
- 使用协议类型与 Agent/Session 的便捷入口：引根包 `pangbit/moonspoa`。

## 环境要求与验证状态

- 开发与本地验证环境为 **macOS arm64**、`moon 0.1.20260920`、`moonc v0.10.14+7d59c7ec9`。这些是已测试版本，尚未确定最低工具链版本。
- 模块声明依赖 `moonbitlang/async@0.22.1`，默认使用 native 后端。native 构建需要 C 编译器与平台开发头文件。
- server 的 C stub 使用 POSIX Unix socket API。**native 不代表支持所有操作系统**：Linux 已在 Ubuntu 24.04（x86_64）上完成验证；当前 server 传输实现不支持 Windows。
- 人工验证覆盖 release 模式测试、文档生成、解压后的发布包，以及独立消费模块的 UDS 完整交互。Linux 上已跑通完整测试套件，并与源码编译的 HAProxy 3.4.4 完成端到端互操作验证：TCP 与 UDS 两种传输、并发 pipelined 流量、`option spop-check` 健康检查（含 agent 宕机 fail-open）及 agent 重启重协商。
- GitHub Actions CI（`ubuntu-24.04`）配置了类型检查、debug/release 测试、格式与生成接口检查。HAProxy 3.4 TCP 冒烟测试覆盖阻断、50 路并发请求及 agent 重启后的转发。文档生成、发布包解压与独立消费模块检查、HAProxy→UDS 互操作、`spop-check` 及宕机 fail-open 仍属于人工验证，不在此 workflow 的覆盖范围内。具体提交的执行结果见 [CI 运行记录](https://github.com/pangbit/moonspoa/actions/workflows/ci.yml)。
- 当前为初始 `0.1.0` 候选版本，上述检查不代表生产成熟度认证。

## 安装

发布到 Mooncakes 后可使用：

```bash
moon add pangbit/moonspoa
```

发布前可克隆本仓库，通过其中的 `moon.work` 运行示例。其他项目若需引用源码，可将两个模块加入本地 workspace，并在消费方的 `moon.mod` 中声明 `"pangbit/moonspoa@0.1.0"`。

## 快速上手

可执行示例需要在模块的 `moon.mod` 中声明 `"pangbit/moonspoa@0.1.0"` 和 `"moonbitlang/async@0.22.1"`，并设置 `preferred_target = "native"`。

### 运行一个 agent 服务器

为 native 可执行包配置 `moon.pkg`：

```text
import {
  "pangbit/moonspoa/agent",
  "pangbit/moonspoa/server",
  "moonbitlang/async",
}
supported_targets = "native"
pkgtype(kind: "executable")
```

然后添加 `main.mbt`：

```mbt nocheck
///|
async fn main {
  let agent = @agent.Agent::new().on("check-ip", fn(msg) {
    // 最小示例接收 demo client 发来的 Str。
    // HAProxy `args ip=src` 发送 Ipv4/Ipv6，见 examples/server。
    guard msg.args is [("ip", Str(ip)), ..] else { return [] }
    // 变量名不含作用域与前缀；HAProxy 侧最终为 txn.<var-prefix>.ip_blocked
    [SetVar(Transaction, "ip_blocked", Bool(ip == "203.0.113.7"))]
  })
  let server = @server.Server::bind(
    agent,
    @server.Listener::tcp("127.0.0.1:12345"),
  )
  defer server.close()
  server.run()
}
```

使用 Unix socket 时，将 TCP listener 替换为 `@server.Unix("/tmp/spoa.sock")`。

### 使用 client 主动访问 SPOA

客户端可执行包的 `moon.pkg`：

```text
import {
  "pangbit/moonspoa/client",
  "moonbitlang/async",
  "moonbitlang/async/socket",
}
supported_targets = "native"
pkgtype(kind: "executable")
```

然后添加 `main.mbt`：

```mbt nocheck
///|
async fn main {
  let conn = @socket.Tcp::connect(@socket.Addr::parse("127.0.0.1:12345"))
  defer conn.close()
  let client = @client.Client::hello(conn, conn) // HELLO 握手 + 协商校验
  let actions = client.notify_messages([
    { name: "check-ip", args: [("ip", Str("203.0.113.7"))], },
  ])
  for action in actions {
    println("\{Repr(action)}")
  }
  ignore(client.disconnect())
}
```

### 并发请求与超时

使用 `with_pipelining` 在并发调用方之间共享连接。专用读任务按 `(stream-id, frame-id)` 分发 ACK，支持乱序响应。未协商 pipelining 时，作用域内通过内部锁串行化请求。作用域外应顺序使用客户端。

下列辅助函数使用上面客户端配置中的 `client` 和 `async` 导入：

```mbt nocheck
///|
async fn query_all(client : @client.Client, ips : Array[String]) -> Unit {
  client.with_pipelining(fn(client) {
    @async.with_task_group(group => {
      for ip in ips {
        group.spawn_bg(() => {
          let actions = client.notify_messages(
            [{ name: "check-ip", args: [("ip", Str(ip))], }],
            timeout=1000,
          )
          println("\{Repr(actions)}")
        })
      }
    })
  })
}
```

`timeout` 单位为毫秒，超时抛出 `SpopError(Timeout)`。等待 ACK 时超时会保留部分读取进度，下次读取可继续；其他 ID 的迟到 ACK 会被丢弃。取消会清理待处理请求登记与串行化锁，退出 `with_pipelining` 会重置模式状态。超时不会撤销 agent 已执行的工作；I/O 故障和连接断开仍需应用层处理恢复。

超过对端 max-frame-size 的 NOTIFY 在写出前抛出 `SpopError(FrameTooBig)`。Agent 会话按协商值限制帧大小，并以 DISCONNECT 报告超限 ACK。在 pipelined 模式中，AGENT-DISCONNECT 会将状态传播给待处理及后续的 NOTIFY 调用。

### 协议层单独使用（根包重导出）

在 `moon.pkg` 中导入 `"pangbit/moonspoa"`：

```mbt check
///|
test {
  let message : @moonspoa.Message = {
    name: "check-ip",
    args: [("ip", Str("1.2.3.4"))],
  }
  let frame = @moonspoa.Frame::notify(0, 1, [message])
  debug_inspect(frame.frame_type, content="Notify")
}
```

## 可运行示例

从仓库根目录，在不同终端执行：

```bash
# 终端 1：IP 黑名单 agent（默认 TCP 127.0.0.1:12345，--unix 可切 UDS，
# --host 0.0.0.0 可接受其它机器上的 HAProxy 连接；
# 默认黑名单为 192.0.2.1 / 198.51.100.23 / 203.0.113.7，
# 每给一个 --block 追加一条，且整体替换默认名单）
moon run examples/server -- --port 12345 --block 203.0.113.7
# 终端 2：demo client 传入待检测 IP（位置参数，缺省 203.0.113.7），
# 发送 check-ip NOTIFY，打印 ACK 的 actions 与判定结论
moon run examples/client -- --port 12345 203.0.113.7
```

server 默认使用 TCP `127.0.0.1:12345`。可用 `--unix PATH` 切换 UDS，用 `--host 0.0.0.0` 接收其他机器的连接。默认黑名单为 `192.0.2.1`、`198.51.100.23`、`203.0.113.7`；一个或多个 `--block IP` 会替换默认名单。client 默认查询 `203.0.113.7`，也支持 `--unix PATH`。

## 接入 HAProxy

`examples/haproxy/` 提供了供验证的完整配置（同样以 IP 黑名单为例）：

- [examples/haproxy/haproxy.cfg](examples/haproxy/haproxy.cfg) — frontend `http-in` 通过
  `filter spoe engine ipblacklist config spoe-ipblacklist.conf` 挂接 agent，
  并以 `http-request deny deny_status 403 if { var(txn.ipbl.ip_blocked) -m bool }`
  拦截命中黑名单的请求；未命中的请求转发到 `127.0.0.1:8000`；
- [examples/haproxy/spoe-ipblacklist.conf](examples/haproxy/spoe-ipblacklist.conf) — SPOE agent 声明（关键片段）：

```
spoe-message check-ip
    args ip=src
    event on-frontend-http-request
```

HAProxy 会把 `src` 以 Ipv4/Ipv6 类型作为 `ip` 参数发送 `check-ip` 消息；agent 回的
`set-var` 动作中，变量名会被加上 `ipbl.` 前缀并置于作用域之后，即 `ip_blocked` 在
HAProxy 中名为 `txn.ipbl.ip_blocked`（Bool）。注意 IPv6 地址在 agent 侧按完整
8 组十六进制形式比较，黑名单条目需写成如
`2001:0db8:0000:0000:0000:0000:0000:0001` 的形式。

验证步骤（已在 Ubuntu 24.04 + HAProxy 3.4.4 实测通过）：

```bash
# 终端 1：agent；本地演示可把 127.0.0.1 加入黑名单以观察 403
moon run examples/server -- --block 127.0.0.1
# 终端 2：haproxy（spoe 配置路径相对 examples/haproxy）
cd examples/haproxy && haproxy -f haproxy.cfg
# 终端 3：命中黑名单 → 403；未命中 → 转发到 127.0.0.1:8000
# （可用 python3 -m http.server 8000 充当后端，未启动时放行请求为 503，属预期）
curl -i http://127.0.0.1:8080/
```

## 测试

```bash
moon check
moon test                 # 模块 preferred_target = native
moon test --release
moon info
moon fmt --check
moon doc
moon package --list       # 检查发布内容，不上传
```

覆盖：varint 边界与溢出、typed-data 与帧 roundtrip、异常帧、内存 duplex 上的 agent/client 会话、pipelining、乱序 ACK、超时与取消恢复，以及真实 TCP 回环 / UDS 集成测试。上面的非测试示例标记为 `nocheck`；英文主文档中的根包示例是可执行文档测试，中文文件不由 MoonBit 自动提取测试。

## 限制与说明

- Agent 并发处理在飞 NOTIFY 任务，写操作串行化。Client 并发调用需要 `with_pipelining`，未协商该能力时请求自动串行化。
- 不实现协议 fragmentation 和 SPOP `async` 能力；无 FIN 的帧以 `FragmentationNotSupported` 拒绝。这与传输读取将一帧拆成多个数据块不同，客户端可以处理后者。
- **UDS accept 每 5 ms 轮询一次**。在声明的 async 依赖下，传输使用 C stub 与公开的 `raw_fd` API，尚无事件驱动的 UDS accept 实现。
- 有符号值遵循 HAProxy 的二进制补码转 UInt64 varint 编码约定；负数使用十字节 varint。
- 在生产环境使用 server 前，请确认上面的平台与验证边界。

## 参考来源与许可

- 协议依据 HAProxy 官方文档 [doc/SPOE.txt](https://github.com/haproxy/haproxy/blob/master/doc/SPOE.txt)（SPOE v1.2）实现。
- varint 与数据类型编码行为参照 HAProxy 源码（`include/haproxy/intops.h`、`include/haproxy/spoe.h`）核对；本库为独立实现。
- 许可证：Apache-2.0，见 [LICENSE](LICENSE)。
