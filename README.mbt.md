# yuebingo/spoa

MoonBit 实现的 SPOA（Stream Processing Offload Agent）库：HAProxy SPOE v1.2 的 SPOP 协议编解码、agent 服务器（TCP / Unix domain socket）与 SPOE 客户端。目标后端：**native**。仓库托管于 <https://github.com/yuebingo/spoa>。

## 包结构与依赖层级

依赖方向严格单向，各子包可独立引用：

```
spop        协议编解码核心，零依赖、后端无关（不 import 任何异步/网络库）
  ↑
agent       agent 侧会话状态机（仅依赖 spop + moonbitlang/async/io，网络层由使用者注入）
client      SPOE/engine 侧客户端（HELLO 协商、顺序/并发 pipelined NOTIFY、超时、优雅断开；也是测试对端）
  ↑
server      TCP / UDS 传输，native only（依赖 agent + moonbitlang/async）

yuebingo/spoa（根包）  便捷重导出 spop 核心类型 + agent 的 Agent/Session
```

- 只要编解码：引 `yuebingo/spoa/spop`；
- 自己接网络：引 `yuebingo/spoa/agent`（或 `client`）；
- 开箱即用的 TCP/UDS 服务器：引 `yuebingo/spoa/server`（**需单独 import**，根包不重导出它，因为它是 native-only 且依赖较重）；
- 一把全要：引根包 `yuebingo/spoa`。

## 安装

```bash
moon add yuebingo/spoa   # 发布到 mooncakes 之后可用
```

发布前也可直接克隆本仓库，以本地路径（path dependency）方式引用。

## 快速上手

### 运行一个 agent 服务器

```mbt nocheck
///|
async fn main {
  let agent = @agent.Agent::new().on("check-ip", fn(msg) {
    // msg.args 是有序的 (String, Data) 列表；HAProxy `args ip=src` 送来的是
    // Ipv4/Ipv6 类型，这里演示 demo client 发来的 Str
    guard msg.args is [("ip", Str(ip)), ..] else { return [] }
    // 变量名不含作用域与前缀；HAProxy 侧最终为 txn.<var-prefix>.ip_blocked
    [SetVar(Transaction, "ip_blocked", Bool(ip == "203.0.113.7"))]
  })
  // TCP：Listener::tcp("127.0.0.1:12345")；UDS：@server.Unix("/tmp/spoa.sock")
  let server = @server.Server::bind(
    agent,
    @server.Listener::tcp("127.0.0.1:12345"),
  )
  server.run() // 每个连接 spawn 一个会话，直到 close() 或 task group 取消
}
```

### 使用 client 主动访问 SPOA

```mbt nocheck
///|
async fn main {
  let conn = @socket.Tcp::connect(@socket.Addr::parse("127.0.0.1:12345"))
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

并发场景用 `with_pipelining` 开启 pipelined 模式（SPOE.txt 3.2.1）：内部启动专用读任务，
按 (stream-id, frame-id) 把 ACK 分发给等待中的 `notify` 调用方，任意数量的任务可共享
同一连接并发收发；ACK 乱序到达也能正确配对。对端未协商 pipelining 时自动退化为
串行在飞，行为依然正确。`timeout` 参数（毫秒）可为单次 notify 设置超时，超时抛
`SpopError(Timeout)`，连接保持可用：

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
          println("\{ip}: \{Repr(actions)}")
        })
      }
    })
  })
}
```

超过对端 max-frame-size 的 NOTIFY 在写出前本地抛 `SpopError(FrameTooBig)`；
对端随时发来的 AGENT-DISCONNECT 会让在飞与后续的 notify 抛出其携带的状态码。

### 协议层单独使用（根包重导出）

```mbt check
///|
test {
  let message : @spoa.Message = {
    name: "check-ip",
    args: [("ip", Str("1.2.3.4"))],
  }
  let frame = @spoa.Frame::notify(0, 1, [message])
  debug_inspect(frame.frame_type, content="Notify")
}
```

### 可运行示例

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

## 接入 HAProxy

`examples/haproxy/` 提供了可直接运行的完整配置（同样以 IP 黑名单为例）：

- `examples/haproxy/haproxy.cfg` — frontend `http-in` 通过
  `filter spoe engine ipblacklist config spoe-ipblacklist.conf` 挂接 agent，
  并以 `http-request deny deny_status 403 if { var(txn.ipbl.ip_blocked) -m bool }`
  拦截命中黑名单的请求；未命中的请求转发到 `127.0.0.1:8000`；
- `examples/haproxy/spoe-ipblacklist.conf` — SPOE agent 声明（关键片段）：

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

端到端演示：

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
moon test        # 模块 preferred_target = native，直接跑即可
```

覆盖：varint 边界向量（逐字节对照 haproxy intops.h 算法）、typed-data 全类型 roundtrip、六种帧型 roundtrip 与错误路径、内存 duplex 上的 agent/client 会话级测试（含 client pipelined 并发、乱序 ACK、超时、异常断开）、真实 TCP 回环与 UDS 集成测试。

## 限制与说明

- **pipelining 已支持**：agent 端并发处理在飞 NOTIFY（写方向加锁）；client 端 `with_pipelining` 作用域内可并发 notify,ACK 按 id 分发、乱序到达亦可配对；对端未协商 pipelining 时自动串行化在飞帧。
- SPOP 的 fragmentation 与 async 能力已被上游废弃，不实现（收到无 FIN 的帧按规范回 `FragmentationNotSupported`）。
- **UDS accept 为轮询实现**（默认 5ms 间隔）：moonbitlang/async@0.22.1 没有公开的 Unix socket API，且其 `internal/*` 包跨模块不可引用（toolchain 强制），因此 UDS 由 C stub 建 socket、经公开包 `raw_fd` 接入事件循环，accept 以短间隔轮询驱动。上游若开放公开 accept readiness API 可直接替换。
- 依赖锁定 `moonbitlang/async@0.22.1`；`server` 包为 native only。
- 有符号整数编码遵循 haproxy 约定：按二进制补码位型 reinterpret 为 UInt64 做 varint 编码（负数为 10 字节 varint），与 haproxy 互通。

## 参考来源与许可

- 协议依据 HAProxy 官方文档 [doc/SPOE.txt](https://github.com/haproxy/haproxy/blob/master/doc/SPOE.txt)（SPOE v1.2）实现。
- varint 与数据类型编码行为参照 HAProxy 源码（`include/haproxy/intops.h`、`include/haproxy/spoe.h`）核对；本库为独立实现，未复制其代码。
- 许可证：Apache-2.0，见 [LICENSE](LICENSE)。
