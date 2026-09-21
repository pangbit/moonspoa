///|
/// A SPOE client: the engine (HAProxy) side of a SPOP connection.
///
/// Created by `Client::hello`, which performs the HELLO handshake. Frames
/// are exchanged sequentially: each `notify` sends a NOTIFY frame and waits
/// for the matching ACK before returning.
///
/// TODO: concurrent pipelined `notify` calls. That requires a dedicated
/// reader task dispatching ACKs to waiting callers, whose lifetime must be
/// bound to a task group owned by the caller (e.g. a scoped
/// `Client::run(reader, writer, fn)`) since moonbitlang/async has no
/// detached tasks.
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
  writer.write(@spop.encode_frame(hello))
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
        writer.write(
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
/// Send a NOTIFY frame with `messages` and wait for the matching ACK.
/// Returns the actions of the ACK. Raises `SpopError` with
/// `FrameIdNotFound` if the ACK echoes different ids, or with the agent's
/// status code if an AGENT-DISCONNECT is received instead.
pub async fn Client::notify(
  self : Client,
  stream_id : UInt64,
  frame_id : UInt64,
  messages : Array[@spop.Message],
) -> Array[@spop.Action] {
  self.writer.write(
    @spop.encode_frame(@spop.Frame::notify(stream_id, frame_id, messages)),
  )
  let frame = self.read_frame()
  match frame.frame_type {
    Ack => {
      if frame.stream_id != stream_id || frame.frame_id != frame_id {
        raise @spop.SpopError::new(
          FrameIdNotFound,
          "ACK echoes (\{frame.stream_id}, \{frame.frame_id}), expected (\{stream_id}, \{frame_id})",
        )
      }
      guard frame.payload is Actions(actions) else {
        raise @spop.SpopError::new(InvalidFrame, "malformed ACK payload")
      }
      actions
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
        "expected ACK, got \{Repr(frame.frame_type)}",
      )
  }
}

///|
/// Disconnect gracefully: send HAPROXY-DISCONNECT with `status` and
/// `message`, and wait for the agent's AGENT-DISCONNECT, whose status code
/// and message are returned.
pub async fn Client::disconnect(
  self : Client,
  status? : @spop.StatusCode = Normal,
  message? : String = "",
) -> (@spop.StatusCode, String) {
  self.writer.write(
    @spop.encode_frame(
      @spop.Frame::disconnect(HAProxyDisconnect, status, message),
    ),
  )
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
