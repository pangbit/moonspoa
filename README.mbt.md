# yuebingo/spoa

MoonBit 实现的 SPOA（Stream Processing Offload Agent）库：HAProxy SPOE v1.2 的 SPOP 协议编解码、agent 服务器（TCP / Unix domain socket）与 SPOE 客户端。目标后端：**native**。仓库托管于 <https://github.com/yuebingo/spoa>。

## 包结构与依赖层级

依赖方向严格单向，各子包可独立引用：

```
spop        协议编解码核心，零依赖、后端无关（不 import 任何异步/网络库）
  ↑
agent       agent 侧会话状态机（仅依赖 spop + moonbitlang/async/io，网络层由使用者注入）
client      SPOE/engine 侧客户端（仅依赖 spop + moonbitlang/async/io，也是测试对端）
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
    // msg.args 是有序的 (String, Data) 列表
    guard msg.args is [("ip", Str(_)), ..] else { return [] }
    // 变量名不含作用域与前缀；HAProxy 侧最终为 txn.<var-prefix>.ip_score
    [SetVar(Transaction, "ip_score", Int32(85))]
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
  let (sid, fid) = client.next_ids()
  let actions = client.notify(sid, fid, [
    { name: "check-ip", args: [("ip", Str("1.2.3.4"))], },
  ])
  for action in actions {
    println("\{Repr(action)}")
  }
  ignore(client.disconnect())
}
```

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
# 终端 1：demo agent（默认 TCP 127.0.0.1:12345，--unix 可切 UDS）
moon run cmd/main -- --port 12345
# 终端 2：demo client，发一条 check-ip NOTIFY 并打印 ACK 的 actions
moon run cmd/client -- --port 12345
```

## HAProxy 侧配置示例

`haproxy.cfg` 片段：

```
global
    maxconn 1024

defaults
    mode http
    timeout connect 5s
    timeout client  30s
    timeout server  30s

frontend http-in
    bind *:8080
    filter spoe engine ipscore config /etc/haproxy/spoe-ipscore.conf
    default_backend servers

backend agents
    mode spop
    balance roundrobin
    server agent1 127.0.0.1:12345
```

`spoe-ipscore.conf` 片段：

```
[ipscore]
spoe-agent agents
    messages check-ip
    option var-prefix ipscore
    timeout processing 10ms
    use-backend agents

spoe-message check-ip
    args ip=src
    event on-frontend-http-request
```

HAProxy 会把 `src` 作为 `ip` 参数发送 `check-ip` 消息；agent 回的 `set-var` 动作中，变量名会被加上 `ipscore.` 前缀并置于作用域之后，即上面的 `ip_score` 在 HAProxy 中名为 `txn.ipscore.ip_score`，可在后续规则中使用，例如 `http-request deny if { var(txn.ipscore.ip_score) -m int gt 80 }`。

## 测试

```bash
moon test        # 模块 preferred_target = native，直接跑即可
```

覆盖：varint 边界向量（逐字节对照 haproxy intops.h 算法）、typed-data 全类型 roundtrip、六种帧型 roundtrip 与错误路径、内存 duplex 上的 agent/client 会话级测试、真实 TCP 回环与 UDS 集成测试。

## 限制与说明

- **pipelining 已支持**：agent 端并发处理在飞 NOTIFY（写方向加锁）；client 端目前是严格顺序的 notify，**并发 pipelined notify 未实现**（需要 scoped 读循环任务，见 `client/client.mbt` 的 TODO）。
- SPOP 的 fragmentation 与 async 能力已被上游废弃，不实现（收到无 FIN 的帧按规范回 `FragmentationNotSupported`）。
- **UDS accept 为轮询实现**（默认 5ms 间隔）：moonbitlang/async@0.22.1 没有公开的 Unix socket API，且其 `internal/*` 包跨模块不可引用（toolchain 强制），因此 UDS 由 C stub 建 socket、经公开包 `raw_fd` 接入事件循环，accept 以短间隔轮询驱动。上游若开放公开 accept readiness API 可直接替换。
- 依赖锁定 `moonbitlang/async@0.22.1`；`server` 包为 native only。
- 有符号整数编码遵循 haproxy 约定：按二进制补码位型 reinterpret 为 UInt64 做 varint 编码（负数为 10 字节 varint），与 haproxy 互通。

## 参考来源与许可

- 协议依据 HAProxy 官方文档 [doc/SPOE.txt](https://github.com/haproxy/haproxy/blob/master/doc/SPOE.txt)（SPOE v1.2）实现。
- varint 与数据类型编码行为参照 HAProxy 源码（`include/haproxy/intops.h`、`include/haproxy/spoe.h`）核对；本库为独立实现，未复制其代码。
- 许可证：Apache-2.0，见 [LICENSE](LICENSE)。
