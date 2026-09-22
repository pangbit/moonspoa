///|
/// Encoders and decoders for the SPOP wire format.
///
/// Frame layout: `<FRAME-LENGTH:4B BE><FRAME-TYPE:1B><FLAGS:4B BE>
/// <STREAM-ID:varint><FRAME-ID:varint><PAYLOAD>`, where FRAME-LENGTH counts
/// everything after itself. FIN is flag bit 0 and must be set (fragmentation
/// is not supported); ABORT is flag bit 1.
///
/// Decoding distinguishes two failure modes:
///
/// - insufficient input is reported as `None` (the caller should read more
///   bytes and retry);
/// - malformed input is reported as a `SpopError` carrying the status code
///   to report to the peer.
///
/// A streaming reader can either buffer bytes and repeatedly call
/// `decode_frame`, or drive the decoder explicitly: read 4 bytes, call
/// `decode_frame_length`, validate against its max-frame-size, read that
/// many bytes and call `decode_frame_body`.

///|
/// Default max-frame-size of the SPOP protocol (16380 bytes).
pub let default_max_frame_size : UInt = 16380

///|
/// Internal decode outcome: either more input bytes are required, or the
/// input is malformed. Converted to `None`/`SpopError` at the public
/// boundary.
priv suberror DecodeError {
  NeedMoreData
  Protocol(code~ : StatusCode, message~ : String)
}

///|
/// Cursor over the bytes being decoded.
priv struct Reader {
  data : BytesView
  mut pos : Int
}

///|
fn Reader::remaining(self : Reader) -> Int {
  self.data.length() - self.pos
}

///|
fn Reader::read_byte(self : Reader) -> Byte raise DecodeError {
  if self.remaining() < 1 {
    raise NeedMoreData
  }
  let b = self.data[self.pos]
  self.pos += 1
  b
}

///|
fn Reader::read_bytes(self : Reader, n : Int) -> BytesView raise DecodeError {
  if self.remaining() < n {
    raise NeedMoreData
  }
  let view = self.data[self.pos:self.pos + n]
  self.pos += n
  view
}

///|
fn Reader::read_u32_be(self : Reader) -> UInt raise DecodeError {
  let b0 = self.read_byte().to_uint()
  let b1 = self.read_byte().to_uint()
  let b2 = self.read_byte().to_uint()
  let b3 = self.read_byte().to_uint()
  (b0 << 24) | (b1 << 16) | (b2 << 8) | b3
}

///|
fn Reader::read_varint(self : Reader) -> UInt64 raise DecodeError {
  let result = decode_varint(self.data[self.pos:]) catch {
    SpopError(code~, message~) => raise Protocol(code~, message~)
  }
  match result {
    None => raise NeedMoreData
    Some((v, consumed)) => {
      self.pos += consumed
      v
    }
  }
}

///|
/// Read a bare `<LENGTH:varint><BYTES>` UTF-8 string.
fn Reader::read_string(self : Reader) -> String raise DecodeError {
  let len = self.read_varint()
  if len > self.remaining().to_uint64() {
    raise NeedMoreData
  }
  let bytes = self.read_bytes(len.to_int())
  @utf8.decode(bytes) catch {
    _ =>
      raise Protocol(
        code=InvalidFrame,
        message="invalid UTF-8 in string of length \{len}",
      )
  }
}

///|
/// Read a TYPED-DATA value (type byte followed by the value).
/// The type byte packs the type id in the low nibble and flags in the high
/// nibble (`type | flags << 4`, matching haproxy's `SPOP_DATA_T_MASK 0x0F` /
/// `SPOP_DATA_FL_MASK 0xF0`); for `Bool` the truth value is flag 0x10.
fn Reader::read_data(self : Reader) -> Data raise DecodeError {
  let type_byte = self.read_byte().to_int()
  match type_byte & 0x0F {
    0 => Null
    1 => Bool((type_byte & 0x10) == 0x10)
    2 => Int32(self.read_varint().reinterpret_as_int64().to_int())
    3 => UInt32(self.read_varint().to_uint())
    4 => Int64(self.read_varint().reinterpret_as_int64())
    5 => UInt64(self.read_varint())
    6 => {
      let bytes = self.read_bytes(4)
      Ipv4(FixedArray::from_iter(bytes.iter()))
    }
    7 => {
      let bytes = self.read_bytes(16)
      Ipv6(FixedArray::from_iter(bytes.iter()))
    }
    8 => Str(self.read_string())
    9 => {
      let len = self.read_varint()
      if len > self.remaining().to_uint64() {
        raise NeedMoreData
      }
      Bin(self.read_bytes(len.to_int()).to_owned())
    }
    id =>
      raise Protocol(code=InvalidFrame, message="unknown data type id \{id}")
  }
}

///|
/// Read a KV-LIST running until the end of the reader's bytes.
fn Reader::read_kv_list(
  self : Reader,
) -> Array[(String, Data)] raise DecodeError {
  let kvs = []
  while self.remaining() > 0 {
    let name = self.read_string()
    let value = self.read_data()
    kvs.push((name, value))
  }
  kvs
}

///|
/// Read a LIST-OF-MESSAGES running until the end of the reader's bytes:
/// `[ <NAME:STRING> <NB-ARGS:1B> <KV-LIST with NB-ARGS entries> ]*`.
fn Reader::read_messages(self : Reader) -> Array[Message] raise DecodeError {
  let messages = []
  while self.remaining() > 0 {
    let name = self.read_string()
    let nb_args = self.read_byte().to_int()
    let args = []
    for _ in 0..<nb_args {
      let key = self.read_string()
      let value = self.read_data()
      args.push((key, value))
    }
    messages.push({ name, args, })
  }
  messages
}

///|
/// Read a LIST-OF-ACTIONS running until the end of the reader's bytes:
/// `[ <ACTION-TYPE:1B> <NB-ARGS:1B> <args...> ]*`. A set-var action's
/// arguments are scope byte + name STRING + value TYPED-DATA; an unset-var
/// action's arguments are scope byte + name STRING.
fn Reader::read_actions(self : Reader) -> Array[Action] raise DecodeError {
  let actions = []
  while self.remaining() > 0 {
    let action_type = self.read_byte().to_int()
    let nb_args = self.read_byte().to_int()
    let scope_id = self.read_byte().to_int()
    guard VarScope::from_int(scope_id) is Some(scope) else {
      raise Protocol(
        code=InvalidFrame,
        message="unknown variable scope id \{scope_id}",
      )
    }
    let name = self.read_string()
    match action_type {
      1 => {
        if nb_args != 3 {
          raise Protocol(
            code=InvalidFrame,
            message="set-var action expects 3 arguments, got \{nb_args}",
          )
        }
        let value = self.read_data()
        actions.push(SetVar(scope, name, value))
      }
      2 => {
        if nb_args != 2 {
          raise Protocol(
            code=InvalidFrame,
            message="unset-var action expects 2 arguments, got \{nb_args}",
          )
        }
        actions.push(UnsetVar(scope, name))
      }
      _ =>
        raise Protocol(
          code=InvalidFrame,
          message="unknown action type \{action_type}",
        )
    }
  }
  actions
}

///|
fn read_frame(r : Reader) -> Frame raise DecodeError {
  let type_id = r.read_byte().to_int()
  guard FrameType::from_int(type_id) is Some(frame_type) else {
    raise Protocol(code=InvalidFrame, message="unknown frame type \{type_id}")
  }
  let flags = r.read_u32_be()
  let fin = (flags & 1) != 0
  if !fin {
    raise Protocol(
      code=FragmentationNotSupported,
      message="received a frame without the FIN flag set",
    )
  }
  let abort = (flags & 2) != 0
  let stream_id = r.read_varint()
  let frame_id = r.read_varint()
  let payload = match frame_type {
    Notify => Messages(r.read_messages())
    Ack => Actions(r.read_actions())
    _ => KeyValues(r.read_kv_list())
  }
  { frame_type, fin, abort, stream_id, frame_id, payload, }
}

///|
/// Write a bare `<LENGTH:varint><BYTES>` UTF-8 string.
fn write_string(buf : Buffer, s : String) -> Unit {
  let bytes = @utf8.encode(s)
  write_varint(buf, bytes.length().to_uint64())
  buf.write_bytes(bytes)
}

///|
/// Write a TYPED-DATA value. The type byte packs the type id in the low
/// nibble and flags in the high nibble (`type | flags << 4`).
fn write_data(buf : Buffer, data : Data) -> Unit raise SpopError {
  let type_id = data.type_id()
  match data {
    Null => buf.write_byte(type_id.to_byte())
    Bool(v) => buf.write_byte((type_id | (if v { 0x10 } else { 0 })).to_byte())
    Int32(v) => {
      buf.write_byte(type_id.to_byte())
      write_varint(buf, v.to_int64().reinterpret_as_uint64())
    }
    UInt32(v) => {
      buf.write_byte(type_id.to_byte())
      write_varint(buf, v.to_uint64())
    }
    Int64(v) => {
      buf.write_byte(type_id.to_byte())
      write_varint(buf, v.reinterpret_as_uint64())
    }
    UInt64(v) => {
      buf.write_byte(type_id.to_byte())
      write_varint(buf, v)
    }
    Ipv4(bytes) => {
      if bytes.length() != 4 {
        raise SpopError::new(InvalidFrame, "IPv4 address must be 4 bytes")
      }
      buf.write_byte(type_id.to_byte())
      buf.write_iter(bytes.iter())
    }
    Ipv6(bytes) => {
      if bytes.length() != 16 {
        raise SpopError::new(InvalidFrame, "IPv6 address must be 16 bytes")
      }
      buf.write_byte(type_id.to_byte())
      buf.write_iter(bytes.iter())
    }
    Str(s) => {
      buf.write_byte(type_id.to_byte())
      write_string(buf, s)
    }
    Bin(bytes) => {
      buf.write_byte(type_id.to_byte())
      write_varint(buf, bytes.length().to_uint64())
      buf.write_bytes(bytes)
    }
  }
}

///|
fn write_kv_list(
  buf : Buffer,
  kvs : Array[(String, Data)],
) -> Unit raise SpopError {
  for kv in kvs {
    write_string(buf, kv.0)
    write_data(buf, kv.1)
  }
}

///|
fn write_messages(
  buf : Buffer,
  messages : Array[Message],
) -> Unit raise SpopError {
  for message in messages {
    if message.args.length() > 255 {
      raise SpopError::new(
        InvalidFrame,
        "message \"\{message.name}\" has more than 255 arguments",
      )
    }
    write_string(buf, message.name)
    buf.write_byte(message.args.length().to_byte())
    write_kv_list(buf, message.args)
  }
}

///|
fn write_actions(buf : Buffer, actions : Array[Action]) -> Unit raise SpopError {
  for action in actions {
    match action {
      SetVar(scope, name, value) => {
        buf.write_byte(b'\x01')
        buf.write_byte(b'\x03')
        buf.write_byte(scope.to_int().to_byte())
        write_string(buf, name)
        write_data(buf, value)
      }
      UnsetVar(scope, name) => {
        buf.write_byte(b'\x02')
        buf.write_byte(b'\x02')
        buf.write_byte(scope.to_int().to_byte())
        write_string(buf, name)
      }
    }
  }
}

///|
/// Encode a TYPED-DATA value (type byte followed by the value).
pub fn encode_data(data : Data) -> Bytes raise SpopError {
  let buf = Buffer(size_hint=16)
  write_data(buf, data)
  buf.to_bytes()
}

///|
/// Decode a TYPED-DATA value from the start of `bytes`. Returns `None` if
/// more bytes are needed; raises `SpopError` on malformed input.
pub fn decode_data(bytes : BytesView) -> Data? raise SpopError {
  let r = Reader::{ data: bytes, pos: 0, }
  let data = r.read_data() catch {
    NeedMoreData => return None
    Protocol(code~, message~) => raise SpopError::new(code, message)
  }
  Some(data)
}

///|
/// Encode a complete frame, including the 4-byte big-endian length prefix.
pub fn encode_frame(frame : Frame) -> Bytes raise SpopError {
  let body = Buffer(size_hint=64)
  body.write_byte(frame.frame_type.to_int().to_byte())
  let flags : UInt = (if frame.fin { 1U } else { 0U }) |
    (if frame.abort { 2U } else { 0U })
  body.write_uint_be(flags)
  write_varint(body, frame.stream_id)
  write_varint(body, frame.frame_id)
  match frame.payload {
    KeyValues(kvs) => write_kv_list(body, kvs)
    Messages(messages) => write_messages(body, messages)
    Actions(actions) => write_actions(body, actions)
  }
  let body_bytes = body.to_bytes()
  let out = Buffer(size_hint=4 + body_bytes.length())
  out.write_uint_be(body_bytes.length().reinterpret_as_uint())
  out.write_bytes(body_bytes)
  out.to_bytes()
}

///|
/// Decode the 4-byte big-endian FRAME-LENGTH prefix. Returns `None` if
/// fewer than 4 bytes are available.
pub fn decode_frame_length(bytes : BytesView) -> UInt? {
  if bytes.length() < 4 {
    return None
  }
  Some(
    (bytes[0].to_uint() << 24) |
    (bytes[1].to_uint() << 16) |
    (bytes[2].to_uint() << 8) |
    bytes[3].to_uint(),
  )
}

///|
/// Decode one frame body (everything after the 4-byte length prefix).
/// `body` must contain exactly one frame body; truncated or trailing bytes
/// raise `SpopError` with `StatusCode::InvalidFrame`.
pub fn decode_frame_body(body : BytesView) -> Frame raise SpopError {
  let r = Reader::{ data: body, pos: 0, }
  let frame = read_frame(r) catch {
    NeedMoreData => raise SpopError::new(InvalidFrame, "truncated frame body")
    Protocol(code~, message~) => raise SpopError::new(code, message)
  }
  if r.pos != body.length() {
    raise SpopError::new(InvalidFrame, "trailing bytes after frame payload")
  }
  frame
}

///|
/// Decode a complete frame (length prefix included) from the start of
/// `bytes`.
///
/// Returns `None` if `bytes` does not yet hold the whole frame (the caller
/// should read more bytes and retry). Raises `SpopError` with
/// `StatusCode::FrameTooBig` if the announced frame length exceeds
/// `max_frame_size`, and with the appropriate status code on malformed
/// input.
pub fn decode_frame(
  bytes : BytesView,
  max_frame_size? : UInt = default_max_frame_size,
) -> Frame? raise SpopError {
  guard decode_frame_length(bytes) is Some(len) else { return None }
  if len > max_frame_size {
    raise SpopError::new(
      FrameTooBig,
      "frame length \{len} exceeds max-frame-size \{max_frame_size}",
    )
  }
  let len = len.reinterpret_as_int()
  if bytes.length() < 4 + len {
    return None
  }
  Some(decode_frame_body(bytes[4:4 + len]))
}
