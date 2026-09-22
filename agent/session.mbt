///|
/// A SPOA agent session on one connection.
///
/// Use `Session::run` to drive the SPOP protocol state machine over an
/// injected `&@io.Reader + &@io.Writer` pair.
pub struct Session {
  agent : Agent
  mut pipelining : Bool
  mut negotiated_max_frame_size : UInt
  mut peer_disconnect : (@spop.StatusCode, String)?
}

///|
pub fn Session::new(agent : Agent) -> Session {
  {
    agent,
    pipelining: false,
    negotiated_max_frame_size: agent.max_frame_size,
    peer_disconnect: None,
  }
}

///|
/// Whether the "pipelining" capability was negotiated with the peer.
pub fn Session::pipelining(self : Session) -> Bool {
  self.pipelining
}

///|
/// The max-frame-size negotiated with the peer (minimum of both announced
/// values).
pub fn Session::negotiated_max_frame_size(self : Session) -> UInt {
  self.negotiated_max_frame_size
}

///|
/// The status code and message of the peer's HAPROXY-DISCONNECT frame, if
/// one was received.
pub fn Session::peer_disconnect(self : Session) -> (@spop.StatusCode, String)? {
  self.peer_disconnect
}

///|
/// Read one frame from `reader`, enforcing the negotiated max-frame-size
/// (which equals the configured own limit until the HELLO handshake
/// completes). Raises `@io.ReaderClosed` on EOF, `SpopError` with
/// `FrameTooBig` if the announced length exceeds the limit, and
/// `SpopError` with `InvalidFrame` on malformed input.
async fn Session::read_frame(
  self : Session,
  reader : &@io.Reader,
) -> @spop.Frame {
  let header = reader.read_exactly(4)
  guard @spop.decode_frame_length(header) is Some(len) else {
    raise @spop.SpopError::new(InvalidFrame, "invalid frame length prefix")
  }
  if len > self.negotiated_max_frame_size {
    raise @spop.SpopError::new(
      FrameTooBig,
      "frame length \{len} exceeds max-frame-size \{self.negotiated_max_frame_size}",
    )
  }
  let body = reader.read_exactly(len.reinterpret_as_int())
  @spop.decode_frame_body(body)
}

///|
/// Run the session to completion: HELLO negotiation, the NOTIFY/ACK loop,
/// and the final disconnect exchange.
///
/// Protocol errors are reported to the peer in an AGENT-DISCONNECT frame
/// carrying the matching status code before the session ends. I/O errors
/// and unexpected EOF produce a best-effort AGENT-DISCONNECT(io error).
/// The session returns normally in all these cases; only cancellation
/// propagates.
pub async fn Session::run(
  self : Session,
  reader : &@io.Reader,
  writer : &@io.Writer,
) -> Unit {
  let write_mutex = @async.Mutex()
  let send = async fn(frame : @spop.Frame) -> Unit {
    let bytes = @spop.encode_frame(frame)
    // FRAME-LENGTH (the 4-byte prefix) does not count towards the limit.
    // DISCONNECT frames are small enough to always fit within the minimum
    // of 256 bytes, so error reporting itself never trips this check.
    if bytes.length() - 4 > self.negotiated_max_frame_size.reinterpret_as_int() {
      raise @spop.SpopError::new(
        FrameTooBig,
        "outgoing \{Repr(frame.frame_type)} frame of \{bytes.length() - 4} bytes exceeds the negotiated max-frame-size \{self.negotiated_max_frame_size}",
      )
    }
    write_mutex.acquire()
    defer write_mutex.release()
    writer.write(bytes)
  }
  self.serve(reader, send) catch {
    @spop.SpopError(code~, message~) =>
      send_quiet(send, @spop.Frame::disconnect(AgentDisconnect, code, message))
    err =>
      send_quiet(
        send,
        @spop.Frame::disconnect(AgentDisconnect, IoError, "I/O error: \{err}"),
      )
  }
}

///|
/// Best-effort frame send: failures are ignored (the connection is going
/// away anyway).
async fn send_quiet(
  send : async (@spop.Frame) -> Unit,
  frame : @spop.Frame,
) -> Unit {
  send(frame) catch {
    _ => ()
  }
}

///|
/// The protocol state machine: HELLO exchange, then the frame loop.
/// Raises `SpopError` on protocol errors and I/O errors from the reader
/// or writer.
async fn Session::serve(
  self : Session,
  reader : &@io.Reader,
  send : async (@spop.Frame) -> Unit,
) -> Unit {
  let first = self.read_frame(reader)
  guard first.frame_type is HAProxyHello else {
    raise @spop.SpopError::new(
      InvalidFrame,
      "expected HAPROXY-HELLO, got \{Repr(first.frame_type)}",
    )
  }
  guard first.payload is KeyValues(args) else {
    raise @spop.SpopError::new(InvalidFrame, "malformed HAPROXY-HELLO payload")
  }
  let info = @spop.parse_hello_args(args)
  let version = @spop.choose_version(info.versions)
  let negotiated = @spop.negotiate_max_frame_size(
    self.agent.max_frame_size,
    info.max_frame_size,
  )
  self.negotiated_max_frame_size = negotiated
  let shared = self.agent.capabilities.filter(c => info.capabilities.contains(c))
  self.pipelining = shared.contains("pipelining")
  send(
    @spop.Frame::hello(AgentHello, [
      ("version", Str(version)),
      // The spec requires the lower of both announced max-frame-size values.
      ("max-frame-size", UInt32(negotiated)),
      ("capabilities", Str(self.agent.capabilities.join(", "))),
    ]),
  )
  if info.healthcheck {
    return
  }
  if self.pipelining {
    self.serve_pipelined(reader, send)
  } else {
    self.serve_sequential(reader, send)
  }
}

///|
/// Frame loop without pipelining: each NOTIFY is answered before the next
/// frame is read.
async fn Session::serve_sequential(
  self : Session,
  reader : &@io.Reader,
  send : async (@spop.Frame) -> Unit,
) -> Unit {
  for ;; {
    let frame = self.read_frame(reader)
    match frame.frame_type {
      Notify => self.handle_notify(frame, send)
      HAProxyDisconnect => {
        self.record_peer_disconnect(frame)
        send(@spop.Frame::disconnect(AgentDisconnect, Normal, ""))
        return
      }
      _ =>
        raise @spop.SpopError::new(
          InvalidFrame,
          "unexpected frame type \{Repr(frame.frame_type)}",
        )
    }
  }
}

///|
/// Frame loop with pipelining: NOTIFY frames are processed concurrently in
/// spawned tasks while the read loop keeps reading; writes are serialized
/// by the caller's mutex. In-flight NOTIFY tasks are awaited before the
/// final AGENT-DISCONNECT is sent.
async fn Session::serve_pipelined(
  self : Session,
  reader : &@io.Reader,
  send : async (@spop.Frame) -> Unit,
) -> Unit {
  @async.with_task_group(group => {
    for ;; {
      let frame = self.read_frame(reader)
      match frame.frame_type {
        Notify => group.spawn_bg(() => self.handle_notify(frame, send))
        HAProxyDisconnect => {
          self.record_peer_disconnect(frame)
          break
        }
        _ =>
          raise @spop.SpopError::new(
            InvalidFrame,
            "unexpected frame type \{Repr(frame.frame_type)}",
          )
      }
    }
  })
  send(@spop.Frame::disconnect(AgentDisconnect, Normal, ""))
}

///|
fn Session::record_peer_disconnect(self : Session, frame : @spop.Frame) -> Unit {
  if frame.payload is KeyValues(args) {
    self.peer_disconnect = Some(
      @spop.parse_disconnect_args(args) catch {
        _ => (UnknownError, "")
      },
    )
  }
}

///|
/// Invoke the handlers for one NOTIFY frame and send the ACK, echoing the
/// stream and frame ids. Unknown message names are skipped. A handler that
/// raises has its message skipped (the error is reported via the agent's
/// `on_error` callback) without affecting the session. An aborted NOTIFY
/// is answered with an empty ACK.
async fn Session::handle_notify(
  self : Session,
  frame : @spop.Frame,
  send : async (@spop.Frame) -> Unit,
) -> Unit {
  guard frame.payload is Messages(messages) else {
    raise @spop.SpopError::new(
      InvalidFrame,
      "NOTIFY frame without message list",
    )
  }
  let actions : Array[@spop.Action] = []
  if !frame.abort {
    for message in messages {
      if self.agent.handlers.get(message.name) is Some(handler) {
        try handler(message) catch {
          err =>
            if self.agent.on_error is Some(on_error) {
              on_error(message.name, err)
            }
        } noraise {
          result => actions.append(result)
        }
      }
    }
  }
  send(@spop.Frame::ack(frame.stream_id, frame.frame_id, actions))
}
