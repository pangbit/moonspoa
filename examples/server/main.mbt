///|
/// Demo SPOA agent server.
///
/// Usage: `moon run examples/server -- [--port N] [--unix PATH]`
/// Defaults to TCP 127.0.0.1:12345.

///|
/// The demo handlers:
/// - "check-ip": reads the "ip" argument and sets the transaction variable
///   `ip_score` to 85, also demonstrating unset-var on the request variable
///   `flag`;
/// - "clear-score": removes `ip_score`.
///
/// Variable names carry no scope or prefix here: HAProxy maps them to
/// `<scope>.<var-prefix>.<name>`, e.g. `txn.ipscore.ip_score`.
fn demo_agent() -> @agent.Agent raise {
  @agent.Agent::new()
  .on("check-ip", fn(msg) {
    guard msg.args is [("ip", Str(_)), ..] else { return [] }
    [SetVar(Transaction, "ip_score", Int32(85)), UnsetVar(Request, "flag")]
  })
  .on("clear-score", fn(_) { [UnsetVar(Transaction, "ip_score")] })
}

///|
fn parse_args(args : Array[String]) -> (String?, Int) raise {
  let mut unix : String? = None
  let mut port = 12345
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
      _ => continue i + 1
    }
  }
  (unix, port)
}

///|
async fn main {
  let (unix, port) = parse_args(@env.args())
  let listener = match unix {
    Some(path) => {
      println("SPOA demo agent listening on unix:\{path}")
      @transport.Unix(path)
    }
    None => {
      println("SPOA demo agent listening on 127.0.0.1:\{port}")
      @transport.Listener::tcp("127.0.0.1:\{port}")
    }
  }
  let server = @transport.Server::bind(demo_agent(), listener)
  server.run()
}
