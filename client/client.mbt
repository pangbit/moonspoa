///|
/// A SPOE client: the engine (HAProxy) side of a SPOP connection.
///
/// Created by `Client::hello`, which performs the HELLO handshake. By
/// default frames are exchanged sequentially: each `notify` sends a NOTIFY
/// frame and waits for the matching ACK before returning.
///
/// `Client::with_pipelining` runs a dedicated reader task that dispatches
/// ACK frames to waiting `notify` callers by their (stream-id, frame-id),
/// so any number of tasks can share the connection with NOTIFY frames in
/// flight simultaneously (the "pipelining" capability, SPOE.txt 3.2.1).
/// When the peer did not negotiate pipelining, in-flight frames are
/// transparently serialized instead, one at a time.
pub struct Client {
  supported_versions : Array[String]
  max_frame_size : UInt
  capabilities : Array[String]
  engine_id : String?
  reader : &@io.Reader
  writer : &@io.Writer
  mut version : String
  mut peer_max_frame_size : UInt
  mut peer_capabilities : Array[String]
  stream_id : UInt64
  mut frame_id : UInt64
  priv write_mutex : @async.Mutex
  priv mut pending : Map[(UInt64, UInt64), @aqueue.Queue[@spop.Frame]]?
  priv mut disconnect_waiter : @aqueue.Queue[@spop.Frame]?
  priv mut terminal : @spop.SpopError?
  priv mut gate : @async.Mutex?
  priv mut stopping : Bool
}

///|
/// The protocol version negotiated with the agent.
pub fn Client::version(self : Client) -> String {
  self.version
}

///|
/// The max-frame-size announced by the agent (the send-side limit).
pub fn Client::peer_max_frame_size(self : Client) -> UInt {
  self.peer_max_frame_size
}

///|
/// The capabilities announced by the agent.
pub fn Client::peer_capabilities(self : Client) -> Array[String] {
  self.peer_capabilities
}

///|
/// Whether both sides announced the "pipelining" capability.
pub fn Client::pipelining(self : Client) -> Bool {
  self.capabilities.contains("pipelining") &&
  self.peer_capabilities.contains("pipelining")
}

///|
/// A fresh pair of stream/frame ids for `Client::notify`. The frame id
/// increases monotonically; the stream id stays 0 (ids only need to be
/// unique among in-flight frames).
pub fn Client::next_ids(self : Client) -> (UInt64, UInt64) {
  self.frame_id += 1
  (self.stream_id, self.frame_id)
}

///|
/// Read one frame from the agent, enforcing our announced max-frame-size.
async fn Client::read_frame(self : Client) -> @spop.Frame {
  let header = self.reader.read_exactly(4)
  guard @spop.decode_frame_length(header) is Some(len) else {
    raise @spop.SpopError::new(InvalidFrame, "invalid frame length prefix")
  }
  if len > self.max_frame_size {
    raise @spop.SpopError::new(
      FrameTooBig,
      "frame length \{len} exceeds max-frame-size \{self.max_frame_size}",
    )
  }
  let body = self.reader.read_exactly(len.reinterpret_as_int())
  @spop.decode_frame_body(body)
}

///|
/// Parse the key-value list of an AGENT-HELLO frame.
fn parse_agent_hello(
  args : Array[(String, @spop.Data)],
) -> (String, UInt, Array[String]) raise @spop.SpopError {
  let mut version : String? = None
  let mut max_frame_size : UInt? = None
  let mut capabilities : Array[String]? = None
  for kv in args {
    match kv {
      ("version", Str(s)) => version = Some(s)
      ("max-frame-size", UInt32(v)) => max_frame_size = Some(v)
      ("capabilities", Str(s)) => capabilities = Some(@spop.parse_list(s))
      _ => ()
    }
  }
  guard version is Some(version) else {
    raise @spop.SpopError::new(
      VersionNotFound,
      "missing version key in AGENT-HELLO frame",
    )
  }
  guard max_frame_size is Some(max_frame_size) else {
    raise @spop.SpopError::new(
      MaxFrameSizeNotFound,
      "missing max-frame-size key in AGENT-HELLO frame",
    )
  }
  guard capabilities is Some(capabilities) else {
    raise @spop.SpopError::new(
      CapabilitiesNotFound,
      "missing capabilities key in AGENT-HELLO frame",
    )
  }
  (version, max_frame_size, capabilities)
}

///|
/// Connect to an agent: send HAPROXY-HELLO and validate the reply.
///
/// On success returns the client with the negotiation results stored. If
/// the agent replies with AGENT-DISCONNECT, raises `SpopError` carrying
/// the agent's status code and message. Raises `SpopError` with
/// `UnsupportedVersion` if the agent picked a version we did not announce,
/// and with `InvalidMaxFrameSize` if the agent announced a max-frame-size
/// larger than ours or smaller than 256 (a HAPROXY-DISCONNECT reporting it
/// is sent first).
pub async fn Client::hello(
  reader : &@io.Reader,
  writer : &@io.Writer,
  supported_versions? : Array[String] = [@spop.supported_version],
  max_frame_size? : UInt = @spop.default_max_frame_size,
  capabilities? : Array[String] = ["pipelining"],
  engine_id? : String? = None,
  healthcheck? : Bool = false,
) -> Client {
  if max_frame_size < @spop.min_frame_size {
    raise @spop.SpopError::new(
      InvalidMaxFrameSize,
      "max-frame-size \{max_frame_size} is smaller than the minimum \{@spop.min_frame_size}",
    )
  }
  let client = {
    supported_versions,
    max_frame_size,
    capabilities,
    engine_id,
    reader,
    writer,
    version: "",
    peer_max_frame_size: max_frame_size,
    peer_capabilities: [],
    stream_id: 0,
    frame_id: 0,
    write_mutex: @async.Mutex(),
    pending: None,
    disconnect_waiter: None,
    terminal: None,
    gate: None,
    stopping: false,
  }
  let hello = @spop.Frame::hello(
    HAProxyHello,
    @spop.hello_args(
      versions=supported_versions,
      max_frame_size~,
      capabilities~,
      healthcheck~,
      engine_id~,
    ),
  )
  let hello_bytes = @spop.encode_frame(hello)
  // FRAME-LENGTH (the 4-byte prefix) does not count towards the frame size.
  if hello_bytes.length() - 4 > max_frame_size.reinterpret_as_int() {
    raise @spop.SpopError::new(
      FrameTooBig,
      "HAPROXY-HELLO frame of \{hello_bytes.length() - 4} bytes exceeds the announced max-frame-size \{max_frame_size}",
    )
  }
  client.send_frame(hello_bytes)
  let frame = client.read_frame()
  match frame.frame_type {
    AgentHello => {
      guard frame.payload is KeyValues(args) else {
        raise @spop.SpopError::new(
          InvalidFrame,
          "malformed AGENT-HELLO payload",
        )
      }
      let (version, peer_max_frame_size, peer_capabilities) = parse_agent_hello(
        args,
      )
      if !supported_versions.contains(version) {
        raise @spop.SpopError::new(
          UnsupportedVersion,
          "agent selected unsupported version \{version}",
        )
      }
      if peer_max_frame_size > max_frame_size ||
        peer_max_frame_size < @spop.min_frame_size {
        let message = "agent announced invalid max-frame-size \{peer_max_frame_size}"
        client.send_frame(
          @spop.encode_frame(
            @spop.Frame::disconnect(
              HAProxyDisconnect,
              InvalidMaxFrameSize,
              message,
            ),
          ),
        ) catch {
          _ => ()
        }
        raise @spop.SpopError::new(InvalidMaxFrameSize, message)
      }
      client.version = version
      client.peer_max_frame_size = peer_max_frame_size
      client.peer_capabilities = peer_capabilities
      if !client.pipelining() {
        // Without a shared "pipelining" capability only one NOTIFY frame
        // may be in flight; the gate serializes concurrent callers.
        client.gate = Some(@async.Mutex())
      }
      client
    }
    AgentDisconnect => {
      guard frame.payload is KeyValues(args) else {
        raise @spop.SpopError::new(
          InvalidFrame,
          "malformed AGENT-DISCONNECT payload",
        )
      }
      let (code, message) = @spop.parse_disconnect_args(args)
      raise @spop.SpopError::new(code, message)
    }
    _ =>
      raise @spop.SpopError::new(
        InvalidFrame,
        "expected AGENT-HELLO or AGENT-DISCONNECT, got \{Repr(frame.frame_type)}",
      )
  }
}

///|
/// Write one encoded frame, serialized against concurrent writers.
async fn Client::send_frame(self : Client, bytes : Bytes) -> Unit {
  self.write_mutex.acquire()
  defer self.write_mutex.release()
  self.writer.write(bytes)
}

///|
/// Send a NOTIFY frame with `messages` and wait for the matching ACK.
/// Returns the actions of the ACK.
///
/// The encoded frame must fit into the max-frame-size announced by the
/// agent; otherwise `SpopError` with `FrameTooBig` is raised before
/// anything is written (fragmentation was removed from SPOP 2.0).
///
/// In sequential mode (outside `Client::with_pipelining`) frames are read
/// until the ACK echoing the request ids arrives; late ACKs of earlier
/// timed-out NOTIFYs are discarded on the way. Inside `with_pipelining`
/// the shared reader task dispatches the ACK by its ids; ids already in
/// flight are rejected with `FrameIdNotFound`, and ACKs for unknown ids
/// (e.g. arriving after a `timeout`) are silently ignored.
///
/// `timeout` is in milliseconds; on expiry `SpopError` with `Timeout` is
/// raised and the connection stays usable. An AGENT-DISCONNECT received
/// while waiting raises `SpopError` with the agent's status code.
pub async fn Client::notify(
  self : Client,
  stream_id : UInt64,
  frame_id : UInt64,
  messages : Array[@spop.Message],
  timeout? : Int,
) -> Array[@spop.Action] {
  let bytes = @spop.encode_frame(
    @spop.Frame::notify(stream_id, frame_id, messages),
  )
  if bytes.length() - 4 > self.peer_max_frame_size.reinterpret_as_int() {
    raise @spop.SpopError::new(
      FrameTooBig,
      "NOTIFY frame of \{bytes.length() - 4} bytes exceeds the agent's max-frame-size \{self.peer_max_frame_size}",
    )
  }
  match timeout {
    Some(ms) =>
      @async.with_timeout(
        ms,
        () => self.wait_ack(stream_id, frame_id, bytes),
        error=@spop.SpopError::new(
          Timeout,
          "NOTIFY (\{stream_id}, \{frame_id}) not acknowledged within \{ms}ms",
        ),
      )
    None => self.wait_ack(stream_id, frame_id, bytes)
  }
}

///|
/// Send a NOTIFY frame with automatically allocated ids; see
/// `Client::notify`.
pub async fn Client::notify_messages(
  self : Client,
  messages : Array[@spop.Message],
  timeout? : Int,
) -> Array[@spop.Action] {
  let (stream_id, frame_id) = self.next_ids()
  match timeout {
    Some(ms) => self.notify(stream_id, frame_id, messages, timeout=ms)
    None => self.notify(stream_id, frame_id, messages)
  }
}

///|
/// Write the encoded NOTIFY frame and wait for its ACK, either directly
/// (sequential mode) or through the reader task (pipelined mode).
async fn Client::wait_ack(
  self : Client,
  stream_id : UInt64,
  frame_id : UInt64,
  bytes : Bytes,
) -> Array[@spop.Action] {
  match self.pending {
    None => {
      self.send_frame(bytes)
      self.wait_ack_sequential(stream_id, frame_id)
    }
    Some(pending) => {
      if self.terminal is Some(err) {
        raise err
      }
      if self.disconnect_waiter is Some(_) {
        raise @spop.SpopError::new(Normal, "client is disconnecting")
      }
      let key = (stream_id, frame_id)
      if pending.contains(key) {
        raise @spop.SpopError::new(
          FrameIdNotFound,
          "frame id (\{stream_id}, \{frame_id}) is already in flight",
        )
      }
      let gate = self.gate
      if gate is Some(g) {
        g.acquire()
      }
      // Cancellation (e.g. from a notify `timeout`) is not catchable, so
      // the cleanup must live in `defer` to run on every exit path.
      defer (if gate is Some(g) { g.release() })
      let waiter : @aqueue.Queue[@spop.Frame] = @aqueue.Queue(
        kind=@aqueue.Unbounded,
      )
      pending[key] = waiter
      defer pending.remove(key)
      // The reader may have terminated between the checks at entry and
      // our registration; re-check now that the waiter is visible to
      // `terminate`.
      if self.terminal is Some(err) {
        raise err
      }
      if self.disconnect_waiter is Some(_) {
        raise @spop.SpopError::new(Normal, "client is disconnecting")
      }
      self.send_frame(bytes)
      let frame = waiter.get()
      guard frame.payload is Actions(actions) else {
        raise @spop.SpopError::new(InvalidFrame, "malformed ACK payload")
      }
      actions
    }
  }
}

///|
/// Sequential-mode ACK wait: read frames until the ACK echoing
/// (stream_id, frame_id) arrives. ACKs echoing other ids are late replies
/// to earlier timed-out NOTIFYs and are discarded, so a `timeout` does
/// not poison the connection.
async fn Client::wait_ack_sequential(
  self : Client,
  stream_id : UInt64,
  frame_id : UInt64,
) -> Array[@spop.Action] {
  for ;; {
    let frame = self.read_frame()
    match frame.frame_type {
      Ack =>
        if frame.stream_id == stream_id && frame.frame_id == frame_id {
          guard frame.payload is Actions(actions) else {
            raise @spop.SpopError::new(InvalidFrame, "malformed ACK payload")
          }
          return actions
        }
      // else: a late ACK for a previously timed-out NOTIFY; discard it
      // and keep reading.
      AgentDisconnect => {
        guard frame.payload is KeyValues(args) else {
          raise @spop.SpopError::new(
            InvalidFrame,
            "malformed AGENT-DISCONNECT payload",
          )
        }
        let (code, message) = @spop.parse_disconnect_args(args)
        raise @spop.SpopError::new(code, message)
      }
      _ =>
        raise @spop.SpopError::new(
          InvalidFrame,
          "expected ACK, got \{Repr(frame.frame_type)}",
        )
    }
  }
}

///|
/// Run `f` with a dedicated reader task that dispatches incoming frames,
/// enabling concurrent in-flight `Client::notify` calls from multiple
/// tasks (the "pipelining" capability). When the peer did not announce
/// "pipelining", in-flight NOTIFY frames are transparently serialized.
///
/// The reader task stops when `f` returns or raises; the client then falls
/// back to sequential mode.
pub async fn[X] Client::with_pipelining(
  self : Client,
  f : async (Client) -> X,
) -> X {
  guard self.pending is None else {
    raise @spop.SpopError::new(
      UnknownError,
      "client is already in pipelining mode",
    )
  }
  let pending : Map[(UInt64, UInt64), @aqueue.Queue[@spop.Frame]] = Map([])
  self.pending = Some(pending)
  // Cancellation is not catchable, so the mode state is reset via `defer`:
  // the client falls back to sequential mode no matter how the scope exits.
  defer {
    self.pending = None
    self.disconnect_waiter = None
    self.stopping = false
  }
  let result : Result[X, Error] = @async.with_task_group(group => {
    // no_wait: the reader loop never ends on its own; it is cancelled when
    // the group winds down (i.e. once `f` and the tasks it awaits are done).
    group.spawn_bg(
      () => self.reader_loop(pending),
      no_wait=true,
      allow_failure=true,
    )
    let result : Result[X, Error] = Ok(f(self)) catch { e => Err(e) }
    // Signal the reader task to exit quietly when the group winds down.
    self.stopping = true
    result
  }) catch {
    e => Err(e)
  }
  match result {
    Ok(value) => value
    Err(err) => raise err
  }
}

///|
/// The pipelined-mode frame reader: dispatches ACK frames to the `notify`
/// callers waiting in `pending` and forwards AGENT-DISCONNECT to a waiting
/// `disconnect`. Never raises: the first terminal condition (EOF, I/O or
/// protocol error, AGENT-DISCONNECT) is recorded in `terminal` and
/// broadcast to all waiters before the loop exits.
async fn Client::reader_loop(
  self : Client,
  pending : Map[(UInt64, UInt64), @aqueue.Queue[@spop.Frame]],
) -> Unit {
  for ;; {
    let frame = self.read_frame() catch {
      err =>
        if self.stopping {
          // The with_pipelining scope is exiting normally.
          return
        } else {
          match err {
            @spop.SpopError(code~, message~) =>
              self.terminate(pending, @spop.SpopError::new(code, message), None)
            _ =>
              self.terminate(
                pending,
                @spop.SpopError::new(IoError, "I/O error: \{err}"),
                None,
              )
          }
          return
        }
    }
    match frame.frame_type {
      Ack =>
        // Unknown ids are ignored: the waiter may have timed out already.
        if pending.get((frame.stream_id, frame.frame_id)) is Some(waiter) {
          ignore(waiter.try_put(frame) catch { _ => false })
        }
      AgentDisconnect => {
        let (code, message) = match frame.payload {
          KeyValues(args) =>
            @spop.parse_disconnect_args(args) catch {
              _ => (UnknownError, "malformed AGENT-DISCONNECT payload")
            }
          _ => (InvalidFrame, "malformed AGENT-DISCONNECT payload")
        }
        self.terminate(
          pending,
          @spop.SpopError::new(code, message),
          Some(frame),
        )
        return
      }
      _ => {
        self.terminate(
          pending,
          @spop.SpopError::new(
            InvalidFrame,
            "unexpected frame type \{Repr(frame.frame_type)}",
          ),
          None,
        )
        return
      }
    }
  }
}

///|
/// Record the terminal error (the first one wins) and wake everyone:
/// pending `notify` waiters get the error; a waiting `disconnect` gets
/// `reply` if provided (the AGENT-DISCONNECT frame), or the error.
fn Client::terminate(
  self : Client,
  pending : Map[(UInt64, UInt64), @aqueue.Queue[@spop.Frame]],
  err : @spop.SpopError,
  reply : @spop.Frame?,
) -> Unit {
  if self.terminal is None {
    self.terminal = Some(err)
  }
  let err = self.terminal.unwrap()
  for entry in pending {
    entry.1.close(error=err)
  }
  if self.disconnect_waiter is Some(waiter) {
    match reply {
      Some(frame) => ignore(waiter.try_put(frame) catch { _ => false })
      None => waiter.close(error=err)
    }
  }
}

///|
/// Disconnect gracefully: send HAPROXY-DISCONNECT with `status` and
/// `message`, and wait for the agent's AGENT-DISCONNECT, whose status code
/// and message are returned.
///
/// In pipelined mode the reply is awaited through the reader task; if the
/// agent already sent AGENT-DISCONNECT, its status code and message are
/// returned directly. Pending `notify` calls are failed by the reader task
/// once the agent's AGENT-DISCONNECT arrives.
pub async fn Client::disconnect(
  self : Client,
  status? : @spop.StatusCode = Normal,
  message? : String = "",
) -> (@spop.StatusCode, String) {
  let bytes = @spop.encode_frame(
    @spop.Frame::disconnect(HAProxyDisconnect, status, message),
  )
  match self.pending {
    None => {
      self.send_frame(bytes)
      let frame = self.read_frame()
      match frame.frame_type {
        AgentDisconnect => {
          guard frame.payload is KeyValues(args) else {
            raise @spop.SpopError::new(
              InvalidFrame,
              "malformed AGENT-DISCONNECT payload",
            )
          }
          @spop.parse_disconnect_args(args)
        }
        _ =>
          raise @spop.SpopError::new(
            InvalidFrame,
            "expected AGENT-DISCONNECT, got \{Repr(frame.frame_type)}",
          )
      }
    }
    Some(_) => {
      if self.terminal is Some(err) {
        return (err.code(), err.message())
      }
      guard self.disconnect_waiter is None else {
        raise @spop.SpopError::new(
          InvalidFrame,
          "disconnect already in progress",
        )
      }
      let waiter : @aqueue.Queue[@spop.Frame] = @aqueue.Queue(
        kind=@aqueue.Unbounded,
      )
      self.disconnect_waiter = Some(waiter)
      // Cancellation is not catchable; reset via `defer` on every exit.
      defer {
        self.disconnect_waiter = None
      }
      self.send_frame(bytes)
      let frame = waiter.get()
      guard frame.payload is KeyValues(args) else {
        raise @spop.SpopError::new(
          InvalidFrame,
          "malformed AGENT-DISCONNECT payload",
        )
      }
      @spop.parse_disconnect_args(args)
    }
  }
}
