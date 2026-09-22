# pangbit/moonspoa

English | [简体中文](README.zh-CN.md)

A MoonBit SPOA (Stream Processing Offload Agent) library implementing the SPOP protocol described in HAProxy's SPOE v1.2 specification. It provides protocol codecs, agent sessions, TCP / Unix domain socket servers, and a SPOE client. The default backend is **native**. [Source repository](https://github.com/pangbit/moonspoa).

## Packages

Packages have one-way dependencies and can be imported separately:

| Package | Purpose | Dependencies |
| --- | --- | --- |
| `pangbit/moonspoa/spop` | Protocol codecs and HELLO negotiation | MoonBit core only; backend independent |
| `pangbit/moonspoa/agent` | Agent configuration and session state machine | `spop`, async runtime and IO; caller supplies transport |
| `pangbit/moonspoa/client` | HELLO, sequential / pipelined NOTIFY, timeouts, DISCONNECT | `spop`, async runtime, IO and queues |
| `pangbit/moonspoa/server` | TCP and Unix domain socket transport | `agent`, async networking and a C stub; native only |
| `pangbit/moonspoa` | Convenience facade | Re-exports `spop` types and `agent`'s `Agent` / `Session`; **not** `server` or `client` |

Import `spop` for codecs alone, `agent` or `client` for a custom transport, and `server` for a ready-to-use listener.

## Requirements and validation status

- Development and local validation used **macOS arm64**, `moon 0.1.20260920` and `moonc v0.10.14+7d59c7ec9`. These are tested versions, not an established minimum toolchain version.
- The module declares `moonbitlang/async@0.22.1` and defaults to the native backend. Native builds require a C compiler and platform development headers.
- The server's C stub uses POSIX Unix socket APIs. **Native does not imply support for every operating system**: Linux has been validated on Ubuntu 24.04 (x86_64); the current server transport does not support Windows.
- Manual validation covers release-mode tests, generated documentation, testing the extracted package, and an independent consumer's UDS round trip. On Linux, the full test suite passes, and end-to-end interoperability with HAProxy 3.4.4 (built from source) has been verified over both TCP and UDS transports, including concurrent pipelined traffic, `option spop-check` health checks with fail-open, and agent restarts.
- GitHub Actions CI (`ubuntu-24.04`) is configured to check types, debug/release tests, formatting and generated interfaces. Its HAProxy 3.4 TCP smoke test checks denial, a 50-request concurrent burst, and forwarding after an agent restart. Documentation generation, extracted-package and independent-consumer checks, HAProxy-to-UDS interoperability, `spop-check`, and outage fail-open remain manual validation; they are not covered by this workflow. See [CI runs](https://github.com/pangbit/moonspoa/actions/workflows/ci.yml) for results tied to a specific commit.
- This is an initial `0.1.0` release candidate. The checks above are not a production-readiness certification.

## Installation

Once the module has been published to Mooncakes:

```bash
moon add pangbit/moonspoa
```

Before publication, clone this repository and run the examples through its `moon.work`. To use the source in another project, add both modules to a local workspace and declare `"pangbit/moonspoa@0.1.0"` in the consumer's `moon.mod`.

## Quick start

For the executable examples, declare both `"pangbit/moonspoa@0.1.0"` and `"moonbitlang/async@0.22.1"` in your module's `moon.mod`, and set `preferred_target = "native"`.

### Agent server

For a native executable package, use this `moon.pkg`:

```text
import {
  "pangbit/moonspoa/agent",
  "pangbit/moonspoa/server",
  "moonbitlang/async",
}
supported_targets = "native"
pkgtype(kind: "executable")
```

Then add `main.mbt`:

```mbt nocheck
///|
async fn main {
  let agent = @agent.Agent::new().on("check-ip", fn(msg) {
    // This minimal example accepts the demo client's string value.
    // HAProxy's `args ip=src` sends Ipv4/Ipv6; see examples/server.
    guard msg.args is [("ip", Str(ip)), ..] else { return [] }
    // HAProxy adds the scope and configured variable prefix.
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

For a Unix socket, replace the TCP listener with `@server.Unix("/tmp/spoa.sock")`.

### SPOE client

The client executable's `moon.pkg`:

```text
import {
  "pangbit/moonspoa/client",
  "moonbitlang/async",
  "moonbitlang/async/socket",
}
supported_targets = "native"
pkgtype(kind: "executable")
```

Then add `main.mbt`:

```mbt nocheck
///|
async fn main {
  let conn = @socket.Tcp::connect(@socket.Addr::parse("127.0.0.1:12345"))
  defer conn.close()
  let client = @client.Client::hello(conn, conn)
  let actions = client.notify_messages([
    { name: "check-ip", args: [("ip", Str("203.0.113.7"))], },
  ])
  for action in actions {
    println("\{Repr(action)}")
  }
  ignore(client.disconnect())
}
```

### Concurrent requests and timeouts

Use `with_pipelining` to share a connection among concurrent callers. A dedicated reader dispatches ACKs by `(stream-id, frame-id)`, including out-of-order replies. If pipelining was not negotiated, an internal gate serializes requests in this scope. Outside the scope, use the client sequentially.

The following helper uses the `client` and `async` imports from the client configuration above:

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

`timeout` is measured in milliseconds and raises `SpopError(Timeout)`. A timeout while waiting for an ACK preserves partial read progress so the next read can resume; late ACKs for other IDs are discarded. Cancellation releases pending-request registrations and the serialization gate, and exiting `with_pipelining` resets its mode state. A timeout does not undo work already performed by the agent. I/O failures and disconnections still require application-level recovery.

Oversized NOTIFY frames raise `SpopError(FrameTooBig)` before being written. Agent sessions enforce the negotiated frame-size limit and report oversized ACKs with a DISCONNECT. In pipelined mode, an AGENT-DISCONNECT propagates its status to pending and subsequent NOTIFY calls.

### Protocol types through the root facade

Import `"pangbit/moonspoa"` in `moon.pkg`:

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

## Runnable examples

Run these from the repository root in separate terminals:

```bash
# Terminal 1: agent server
moon run examples/server -- --port 12345 --block 203.0.113.7
# Terminal 2: send an IP and print the ACK actions and verdict
moon run examples/client -- --port 12345 203.0.113.7
```

The server defaults to TCP `127.0.0.1:12345`. Use `--unix PATH` for UDS or `--host 0.0.0.0` to accept connections from other machines. Its default blacklist is `192.0.2.1`, `198.51.100.23`, and `203.0.113.7`; supplying one or more `--block IP` arguments replaces that list. The client defaults to IP `203.0.113.7` and also accepts `--unix PATH`.

## HAProxy integration

[examples/haproxy/haproxy.cfg](examples/haproxy/haproxy.cfg) connects the `http-in` frontend to the agent using `filter spoe engine ipblacklist config spoe-ipblacklist.conf`. It denies requests when `txn.ipbl.ip_blocked` is true and forwards other requests to `127.0.0.1:8000`.

The message in [spoe-ipblacklist.conf](examples/haproxy/spoe-ipblacklist.conf) is:

```text
spoe-message check-ip
    args ip=src
    event on-frontend-http-request
```

HAProxy sends `src` as an Ipv4/Ipv6 argument. The runnable server supports these types as well as the demo client's string values. HAProxy adds the configured `ipbl` prefix and transaction scope to the returned `ip_blocked` variable, producing `txn.ipbl.ip_blocked`.

The demo compares IPv6 addresses in full eight-group hexadecimal form; use blacklist entries such as `2001:0db8:0000:0000:0000:0000:0000:0001`.

To try the configuration (verified end-to-end with HAProxy 3.4.4 on Ubuntu 24.04):

```bash
# Terminal 1: block the local client's address
moon run examples/server -- --block 127.0.0.1
# Terminal 2: paths in the HAProxy configuration are relative to this directory
cd examples/haproxy && haproxy -f haproxy.cfg
# Terminal 3: a matching address should receive 403
curl -i http://127.0.0.1:8080/
```

Allowed requests need a backend on port 8000; for a local demo, run `python3 -m http.server 8000`. Without it, forwarded requests receive 503.

## Validation

```bash
moon check
moon test                 # native is the module's preferred target
moon test --release
moon info
moon fmt --check
moon doc
moon package --list       # inspect publication contents without uploading
```

Tests cover varint boundary and overflow cases, typed-data and frame round trips, malformed frames, in-memory agent/client sessions, pipelining, out-of-order ACKs, timeout/cancellation recovery, and real TCP loopback / UDS integration. Examples and non-test snippets above are marked `nocheck`; the root-facade test is executable documentation.

## Limitations

- The agent processes in-flight NOTIFY tasks with serialized writes. Client concurrency requires `with_pipelining`; negotiation falls back to serialized requests when needed.
- Protocol fragmentation and the SPOP `async` capability are not implemented. Frames without FIN are rejected with `FragmentationNotSupported`. This is separate from ordinary transport reads splitting a frame into chunks, which the client handles.
- UDS accept polls at a **5 ms interval**. With the declared async dependency, the transport uses a C stub and the public `raw_fd` API; it does not have an event-driven UDS accept implementation.
- Signed values follow HAProxy's two's-complement-to-UInt64 varint convention; negative values use ten-byte varints.
- See the platform and validation boundaries above before adopting the server in production.

## References and license

- Protocol reference: HAProxy [doc/SPOE.txt](https://github.com/haproxy/haproxy/blob/master/doc/SPOE.txt), SPOE v1.2.
- Varint and typed-data encoding were checked against HAProxy's `include/haproxy/intops.h` and `include/haproxy/spoe.h`. This library is an independent implementation.
- Licensed under Apache-2.0; see [LICENSE](LICENSE).
