///|
/// Demo SPOE client: connects to the demo agent (`examples/server`), sends one
/// "check-ip" NOTIFY and prints the actions of the ACK.
///
/// Usage: `moon run examples/client -- [--port N] [--unix PATH]`
/// Defaults to TCP 127.0.0.1:12345.

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
async fn run_client(reader : &@io.Reader, writer : &@io.Writer) -> Unit {
  let client = @spoe.Client::hello(reader, writer)
  println("connected, negotiated version \{client.version()}")
  let (sid, fid) = client.next_ids()
  let actions = client.notify(sid, fid, [
    { name: "check-ip", args: [("ip", Str("1.2.3.4"))], },
  ])
  println("received \{actions.length()} action(s):")
  for action in actions {
    println("  \{Repr(action)}")
  }
  let (code, message) = client.disconnect()
  println("disconnected: \{Repr(code)} \{message}")
}

///|
async fn main {
  let (unix, port) = parse_args(@env.args())
  match unix {
    Some(path) => {
      let stream = @server.connect_unix(path)
      run_client(stream, stream)
    }
    None => {
      let conn = @socket.Tcp::connect(@socket.Addr::parse("127.0.0.1:\{port}"))
      run_client(conn, conn)
    }
  }
}
