///|
/// Type definitions of the SPOP protocol (HAProxy SPOE, doc/SPOE.txt v1.2).

///|
/// A typed value carried by the SPOP protocol (TYPED-DATA).
///
/// On the wire the type id occupies the low 4 bits of the first byte and
/// the flags the high 4 bits; for `Bool` the truth value is flag 0x10
/// (haproxy's `SPOP_DATA_FL_TRUE`).
///
/// Signed integers (`Int32`/`Int64`) are encoded by reinterpreting their
/// two's complement bit pattern as an unsigned 64-bit integer which is then
/// varint-encoded (a negative `Int64` therefore occupies a 10-byte varint),
/// matching haproxy's `spoe_encode_data`/`spoe_decode_data`. Decoding reads
/// an unsigned 64-bit varint and reinterprets/truncates it.
pub(all) enum Data {
  Null
  Bool(Bool)
  Int32(Int)
  UInt32(UInt)
  Int64(Int64)
  UInt64(UInt64)
  Ipv4(FixedArray[Byte]) // 4 bytes
  Ipv6(FixedArray[Byte]) // 16 bytes
  Str(String)
  Bin(Bytes)
} derive(Eq, Debug)

///|
/// The numeric type id of a `Data` value (high 4 bits of the type byte).
fn Data::type_id(self : Data) -> Int {
  match self {
    Null => 0
    Bool(_) => 1
    Int32(_) => 2
    UInt32(_) => 3
    Int64(_) => 4
    UInt64(_) => 5
    Ipv4(_) => 6
    Ipv6(_) => 7
    Str(_) => 8
    Bin(_) => 9
  }
}

///|
/// Scope of a variable set or unset by an `Action`.
pub(all) enum VarScope {
  Process
  Session
  Transaction
  Request
  Response
} derive(Eq, Debug)

///|
/// The numeric id of a variable scope on the wire.
pub fn VarScope::to_int(self : VarScope) -> Int {
  match self {
    Process => 0
    Session => 1
    Transaction => 2
    Request => 3
    Response => 4
  }
}

///|
/// Convert a wire scope id to a `VarScope`; `None` for unknown ids.
pub fn VarScope::from_int(id : Int) -> VarScope? {
  match id {
    0 => Some(Process)
    1 => Some(Session)
    2 => Some(Transaction)
    3 => Some(Request)
    4 => Some(Response)
    _ => None
  }
}

///|
/// An action carried by an ACK frame, asking HAProxy to set or unset a
/// variable. `SetVar` is action type 1 (3 arguments: scope, name, value),
/// `UnsetVar` is action type 2 (2 arguments: scope, name).
pub(all) enum Action {
  SetVar(VarScope, String, Data)
  UnsetVar(VarScope, String)
} derive(Eq, Debug)

///|
/// A message carried by a NOTIFY frame: a name plus an ordered list of
/// named arguments (the protocol encodes arguments in order).
pub(all) struct Message {
  name : String
  args : Array[(String, Data)]
} derive(Eq, Debug)

///|
/// Type of a SPOP frame.
pub(all) enum FrameType {
  HAProxyHello
  HAProxyDisconnect
  Notify
  AgentHello
  AgentDisconnect
  Ack
} derive(Eq, Debug)

///|
/// The numeric frame type id on the wire.
pub fn FrameType::to_int(self : FrameType) -> Int {
  match self {
    HAProxyHello => 1
    HAProxyDisconnect => 2
    Notify => 3
    AgentHello => 101
    AgentDisconnect => 102
    Ack => 103
  }
}

///|
/// Convert a wire frame type id to a `FrameType`; `None` for unknown ids.
pub fn FrameType::from_int(id : Int) -> FrameType? {
  match id {
    1 => Some(HAProxyHello)
    2 => Some(HAProxyDisconnect)
    3 => Some(Notify)
    101 => Some(AgentHello)
    102 => Some(AgentDisconnect)
    103 => Some(Ack)
    _ => None
  }
}

///|
/// Status code reported in DISCONNECT frames and carried by `SpopError`.
pub(all) enum StatusCode {
  Normal
  IoError
  Timeout
  FrameTooBig
  InvalidFrame
  VersionNotFound
  MaxFrameSizeNotFound
  CapabilitiesNotFound
  UnsupportedVersion
  InvalidMaxFrameSize
  FragmentationNotSupported
  InvalidInterlacedFrames
  FrameIdNotFound
  ResourceAllocError
  UnknownError
} derive(Eq, Debug)

///|
/// The numeric status code on the wire.
pub fn StatusCode::to_uint(self : StatusCode) -> UInt {
  match self {
    Normal => 0
    IoError => 1
    Timeout => 2
    FrameTooBig => 3
    InvalidFrame => 4
    VersionNotFound => 5
    MaxFrameSizeNotFound => 6
    CapabilitiesNotFound => 7
    UnsupportedVersion => 8
    InvalidMaxFrameSize => 9
    FragmentationNotSupported => 10
    InvalidInterlacedFrames => 11
    FrameIdNotFound => 12
    ResourceAllocError => 13
    UnknownError => 99
  }
}

///|
/// Convert a wire status code to a `StatusCode`; `None` for unknown codes.
pub fn StatusCode::from_uint(code : UInt) -> StatusCode? {
  match code {
    0 => Some(Normal)
    1 => Some(IoError)
    2 => Some(Timeout)
    3 => Some(FrameTooBig)
    4 => Some(InvalidFrame)
    5 => Some(VersionNotFound)
    6 => Some(MaxFrameSizeNotFound)
    7 => Some(CapabilitiesNotFound)
    8 => Some(UnsupportedVersion)
    9 => Some(InvalidMaxFrameSize)
    10 => Some(FragmentationNotSupported)
    11 => Some(InvalidInterlacedFrames)
    12 => Some(FrameIdNotFound)
    13 => Some(ResourceAllocError)
    99 => Some(UnknownError)
    _ => None
  }
}

///|
/// The payload of a SPOP frame, whose shape depends on the frame type:
/// HELLO and DISCONNECT frames carry a key-value list, NOTIFY frames carry
/// a list of messages, and ACK frames carry a list of actions.
pub(all) enum FramePayload {
  KeyValues(Array[(String, Data)])
  Messages(Array[Message])
  Actions(Array[Action])
} derive(Eq, Debug)

///|
/// A SPOP frame: `<FRAME-TYPE><FLAGS><STREAM-ID><FRAME-ID><PAYLOAD>`.
///
/// `fin` is flag bit 0 and must be set on every frame (fragmentation is not
/// supported); `abort` is flag bit 1. `stream_id` and `frame_id` are
/// varint-encoded on the wire.
pub(all) struct Frame {
  frame_type : FrameType
  fin : Bool
  abort : Bool
  stream_id : UInt64
  frame_id : UInt64
  payload : FramePayload
} derive(Eq, Debug)

///|
/// Build a HAPROXY-HELLO or AGENT-HELLO frame carrying `args`.
pub fn Frame::hello(
  frame_type : FrameType,
  args : Array[(String, Data)],
) -> Frame {
  {
    frame_type,
    fin: true,
    abort: false,
    stream_id: 0,
    frame_id: 0,
    payload: KeyValues(args),
  }
}

///|
/// Build a NOTIFY frame carrying `messages`.
pub fn Frame::notify(
  stream_id : UInt64,
  frame_id : UInt64,
  messages : Array[Message],
) -> Frame {
  {
    frame_type: Notify,
    fin: true,
    abort: false,
    stream_id,
    frame_id,
    payload: Messages(messages),
  }
}

///|
/// Build an ACK frame carrying `actions`, echoing the stream and frame ids
/// of the NOTIFY it answers.
pub fn Frame::ack(
  stream_id : UInt64,
  frame_id : UInt64,
  actions : Array[Action],
) -> Frame {
  {
    frame_type: Ack,
    fin: true,
    abort: false,
    stream_id,
    frame_id,
    payload: Actions(actions),
  }
}

///|
/// Build a HAPROXY-DISCONNECT or AGENT-DISCONNECT frame reporting
/// `status_code` and `message`.
pub fn Frame::disconnect(
  frame_type : FrameType,
  status_code : StatusCode,
  message : String,
) -> Frame {
  {
    frame_type,
    fin: true,
    abort: false,
    stream_id: 0,
    frame_id: 0,
    payload: KeyValues(disconnect_args(status_code, message)),
  }
}

///|
/// The key-value list carried by a DISCONNECT frame for `status_code` and
/// `message`: `status-code` (UINT32) and `message` (STRING).
pub fn disconnect_args(
  status_code : StatusCode,
  message : String,
) -> Array[(String, Data)] {
  [("status-code", UInt32(status_code.to_uint())), ("message", Str(message))]
}

///|
/// Extract the status code and message from the key-value list of a
/// DISCONNECT frame. Missing keys default to `StatusCode::UnknownError`
/// and an empty message; entries of the wrong type raise
/// `StatusCode::InvalidFrame`.
pub fn parse_disconnect_args(
  args : Array[(String, Data)],
) -> (StatusCode, String) raise SpopError {
  let mut code = UnknownError
  let mut message = ""
  for kv in args {
    match kv {
      ("status-code", UInt32(v)) =>
        code = StatusCode::from_uint(v).unwrap_or(UnknownError)
      ("status-code", _) =>
        raise SpopError::new(InvalidFrame, "status-code must be a UINT32")
      ("message", Str(s)) => message = s
      ("message", _) =>
        raise SpopError::new(InvalidFrame, "message must be a STRING")
      _ => ()
    }
  }
  (code, message)
}
