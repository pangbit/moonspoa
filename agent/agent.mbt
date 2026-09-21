///|
/// Configuration of a SPOA agent: the protocol parameters announced in
/// AGENT-HELLO and the message handlers invoked for NOTIFY frames.
pub(all) struct Agent {
  /// The SPOP version this agent speaks. Only "2.0" is supported.
  version : String
  /// Maximum frame size this agent accepts, announced in AGENT-HELLO.
  max_frame_size : UInt
  /// Capabilities announced in AGENT-HELLO ("pipelining" by default).
  capabilities : Array[String]
  /// Message handlers, keyed by message name.
  handlers : Map[String, (@spop.Message) -> Array[@spop.Action] raise]
  /// Optional callback invoked when a message handler raises an error;
  /// the message is skipped and the session stays alive.
  on_error : ((String, Error) -> Unit)?
}

///|
/// Create an agent configuration.
///
/// Raises `SpopError` with `UnsupportedVersion` if `version` is not
/// supported by this library, and with `InvalidMaxFrameSize` if
/// `max_frame_size` is smaller than 256.
pub fn Agent::new(
  version? : String = @spop.supported_version,
  max_frame_size? : UInt = @spop.default_max_frame_size,
  capabilities? : Array[String] = ["pipelining"],
  on_error? : (String, Error) -> Unit,
) -> Agent raise @spop.SpopError {
  ignore(@spop.choose_version([version]))
  if max_frame_size < @spop.min_frame_size {
    raise @spop.SpopError::new(
      InvalidMaxFrameSize,
      "max-frame-size \{max_frame_size} is smaller than the minimum \{@spop.min_frame_size}",
    )
  }
  { version, max_frame_size, capabilities, handlers: {}, on_error, }
}

///|
/// Register a handler for messages named `name`. Returns the agent for
/// builder-style chaining.
pub fn Agent::on(
  self : Agent,
  name : String,
  handler : (@spop.Message) -> Array[@spop.Action] raise,
) -> Agent {
  self.handlers[name] = handler
  self
}
