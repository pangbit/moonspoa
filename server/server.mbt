///|
/// A listening endpoint of a SPOA server.
pub(all) enum Listener {
  /// Listen on a TCP address, e.g. `@socket.Addr::parse("127.0.0.1:12345")`.
  Tcp(@socket.Addr)
  /// Listen on a Unix domain socket at the given path.
  Unix(String)
}

///|
/// Convenience constructor for a TCP listener from a "host:port" string.
pub fn Listener::tcp(addr : String) -> Listener raise {
  Tcp(@socket.Addr::parse(addr))
}

///|
/// Interval in milliseconds between two polls of the Unix domain socket
/// accept loop. moonbitlang/async@0.22.1 exposes no public accept
/// readiness API, so UDS connections are accepted with this much added
/// latency at most.
let accept_poll_interval_ms : Int = 5

///|
/// A SPOA server: accepts connections on a `Listener` and runs one agent
/// session per connection. Create it with `Server::bind`, accept with
/// `Server::run`, shut it down with `Server::close` (or by cancelling the
/// task group `run` executes in).
pub struct Server {
  agent : @agent.Agent
  listener : Listener
  mut tcp_server : @socket.TcpServer?
  mut uds_listener : @raw_fd.RawFd?
  mut closed : Bool
}

///|
/// Bind the listener without accepting connections yet. Binding up front
/// (rather than inside `run`) makes the effective address available via
/// `Server::addr` before any client connects, e.g. when binding TCP port 0.
pub async fn Server::bind(agent : @agent.Agent, listener : Listener) -> Server {
  let server = {
    agent,
    listener,
    tcp_server: None,
    uds_listener: None,
    closed: false,
  }
  match listener {
    Tcp(addr) => server.tcp_server = Some(@socket.TcpServer(addr))
    Unix(path) => {
      let fd = uds_listen_ffi(@utf8.encode(path), 128)
      if fd < 0 {
        raise UdsListen(path~, errno=-fd)
      }
      server.uds_listener = Some(@raw_fd.RawFd(fd))
    }
  }
  server
}

///|
/// The actual bound TCP address (with the OS-assigned port when bound to
/// port 0); `None` for Unix listeners.
pub fn Server::addr(self : Server) -> @socket.Addr? {
  self.tcp_server.map(server => server.addr)
}

///|
/// Close the listener and, for Unix listeners, remove the socket file.
/// In-flight sessions keep running; the accept loop in `run` ends.
pub fn Server::close(self : Server) -> Unit {
  if self.closed {
    return
  }
  self.closed = true
  if self.tcp_server is Some(tcp_server) {
    tcp_server.close()
  }
  if self.uds_listener is Some(listener) {
    listener.close()
    if self.listener is Unix(path) {
      ignore(uds_unlink_ffi(@utf8.encode(path)))
    }
  }
}

///|
/// Accept connections and spawn an agent session per connection, until the
/// server is closed via `Server::close` or the surrounding task group is
/// cancelled. Sessions run inside a nested task group, so `run` only
/// returns after the in-flight sessions have ended.
pub async fn Server::run(self : Server) -> Unit {
  if self.tcp_server is Some(tcp_server) {
    tcp_server.run_forever((conn, _addr) => {
      @agent.Session::new(self.agent).run(conn, conn)
    }) catch {
      // Closing the listener fails `accept`; that is the normal shutdown
      // path.
      err => if !self.closed { raise err }
    }
  } else if self.uds_listener is Some(listener) {
    @async.with_task_group(group => {
      while !self.closed {
        let fd = uds_accept_ffi(listener.fd)
        if fd > 0 {
          let stream = @raw_fd.RawFdStream(fd)
          group.spawn_bg(allow_failure=true, () => {
            defer stream.close()
            @agent.Session::new(self.agent).run(stream, stream)
          })
        } else if fd == 0 {
          // No pending connection. There is no public accept-readiness
          // API, so poll with a short sleep; the sleep also makes the
          // loop respond to cancellation.
          @async.sleep(accept_poll_interval_ms)
        } else {
          raise UdsAccept(errno=-fd)
        }
      }
    })
  }
}
