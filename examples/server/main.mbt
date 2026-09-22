///|
/// Demo SPOA agent server: an IP blacklist.
///
/// Usage: `moon run examples/server -- [--port N] [--unix PATH] [--block IP ...]`
/// Defaults to TCP 127.0.0.1:12345. Each `--block` adds a blacklist entry;
/// when at least one is given, the entries replace the built-in default list.

///|
/// The demo handler answers "check-ip" messages: it reads the "ip" argument
/// and sets the transaction variable `ip_blocked` to whether the address is
/// on the blacklist. HAProxy sends `args ip=src` as Ipv4/Ipv6 typed data
/// while the demo client sends a Str, so all three are accepted; Ipv6
/// addresses are compared in their full 8-group form (e.g. "2001:0db8:...").
///
/// Variable names carry no scope or prefix here: HAProxy maps them to
/// `<scope>.<var-prefix>.<name>`, e.g. `txn.ipbl.ip_blocked`.
fn blacklist_agent(blacklist : Array[String]) -> @agent.Agent raise {
  @agent.Agent::new().on("check-ip", fn(msg) {
    guard msg.args is [("ip", data), ..] else { return [] }
    let ip = match data {
      Str(s) => s
      Ipv4(b) =>
        "\{b[0].to_int()}.\{b[1].to_int()}.\{b[2].to_int()}.\{b[3].to_int()}"
      Ipv6(b) => ipv6_to_string(b)
      _ => return []
    }
    let blocked = blacklist.contains(ip)
    let verdict = if blocked { "blocked" } else { "allowed" }
    println("check-ip \{ip} -> \{verdict}")
    [SetVar(Transaction, "ip_blocked", Bool(blocked))]
  })
}

///|
/// Format 16 raw bytes as a full-form IPv6 address: 8 groups of 4 lowercase
/// hex digits (e.g. "2001:0db8:0000:0000:0000:0000:0000:0001").
fn ipv6_to_string(b : FixedArray[Byte]) -> String {
  let groups : Array[String] = []
  for i = 0; i < 8; i = i + 1 {
    groups.push(b[2 * i].to_hex() + b[2 * i + 1].to_hex())
  }
  groups.join(":")
}

///|
/// Returns `(unix_path, port, cli_blacklist)`; an empty `cli_blacklist`
/// means no `--block` was given and the default list applies.
fn parse_args(args : Array[String]) -> (String?, Int, Array[String]) raise {
  let mut unix : String? = None
  let mut port = 12345
  let blocked : Array[String] = []
  for i = 1; i < args.length(); {
    match args[i] {
      "--unix" if i + 1 < args.length() => {
        unix = Some(args[i + 1])
        continue i + 2
      }
      "--port" if i + 1 < args.length() => {
        port = @string.parse_int(args[i + 1])
        continue i + 2
      }
      "--block" if i + 1 < args.length() => {
        blocked.push(args[i + 1])
        continue i + 2
      }
      _ => continue i + 1
    }
  }
  (unix, port, blocked)
}

///|
async fn main {
  let (unix, port, cli_blacklist) = parse_args(@env.args())
  let blacklist = if cli_blacklist.is_empty() {
    ["192.0.2.1", "198.51.100.23", "203.0.113.7"]
  } else {
    cli_blacklist
  }
  let listener = match unix {
    Some(path) => {
      println("SPOA IP-blacklist agent listening on unix:\{path}")
      @transport.Unix(path)
    }
    None => {
      println("SPOA IP-blacklist agent listening on 127.0.0.1:\{port}")
      @transport.Listener::tcp("127.0.0.1:\{port}")
    }
  }
  println("blacklist: \{blacklist.join(", ")}")
  let server = @transport.Server::bind(blacklist_agent(blacklist), listener)
  server.run()
}
