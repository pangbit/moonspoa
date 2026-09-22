///|
/// Demo SPOE client: connects to the demo agent (`examples/server`), sends one
/// "check-ip" NOTIFY for the given IP and prints the verdict from the ACK.
///
/// Usage: `moon run examples/client -- [--port N] [--unix PATH] [IP]`
/// Defaults to TCP 127.0.0.1:12345 and IP 203.0.113.7 (which is on the
/// server's default blacklist, so the demo shows a BLOCKED verdict).

///|
fn parse_args(args : Array[String]) -> (String?, Int, String) raise {
  let mut unix : String? = None
  let mut port = 12345
  let mut ip : String? = None
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
      s if !s.has_prefix("--") => {
        // First positional argument is the IP to check; extras are ignored.
        if ip is None {
          ip = Some(s)
        }
        continue i + 1
      }
      _ => continue i + 1
    }
  }
  (unix, port, ip.unwrap_or("203.0.113.7"))
}

///|
async fn run_client(
  reader : &@io.Reader,
  writer : &@io.Writer,
  ip : String,
) -> Unit {
  let client = @spoe.Client::hello(reader, writer)
  println("connected, negotiated version \{client.version()}")
  let (sid, fid) = client.next_ids()
  let actions = client.notify(sid, fid, [
    { name: "check-ip", args: [("ip", Str(ip))], },
  ])
  println("received \{actions.length()} action(s):")
  for action in actions {
    println("  \{Repr(action)}")
  }
  let mut verdict = "unknown (no ip_blocked variable in the ACK)"
  for action in actions {
    if action is SetVar(Transaction, "ip_blocked", Bool(blocked)) {
      verdict = if blocked { "BLOCKED" } else { "allowed" }
    }
  }
  println("verdict: \{ip} is \{verdict}")
  let (code, message) = client.disconnect()
  println("disconnected: \{Repr(code)} \{message}")
}

///|
async fn main {
  let (unix, port, ip) = parse_args(@env.args())
  match unix {
    Some(path) => {
      let stream = @server.connect_unix(path)
      run_client(stream, stream, ip)
    }
    None => {
      let conn = @socket.Tcp::connect(@socket.Addr::parse("127.0.0.1:\{port}"))
      run_client(conn, conn, ip)
    }
  }
}
