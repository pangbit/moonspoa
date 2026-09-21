///|
/// Black-box tests for the SPOP protocol codec: typed data, frames and the
/// HELLO negotiation helpers, plus malformed-input error paths.

///|
test "data roundtrip for all types" {
  let cases : Array[@spop.Data] = [
    Null,
    Bool(true),
    Bool(false),
    Int32(0),
    Int32(1),
    Int32(-1),
    Int32(-12345),
    Int32(2147483647),
    Int32(-2147483648),
    UInt32(0),
    UInt32(4294967295),
    Int64(0),
    Int64(-1),
    Int64(9223372036854775807),
    Int64(-9223372036854775808),
    UInt64(18446744073709551615),
    Ipv4([127, 0, 0, 1]),
    Ipv6([0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
    Str(""),
    Str("hello"),
    Str("中文"),
    Bin(b"\x00\x01\xff"),
  ]
  for data in cases {
    assert_eq(@spop.decode_data(@spop.encode_data(data)), Some(data))
  }
}

///|
test "negative signed integers encode as 10-byte varints" {
  // haproxy convention: signed values are reinterpreted as their two's
  // complement UInt64 bit pattern before varint encoding.
  let encoded64 = @spop.encode_data(Int64(-1))
  assert_eq(encoded64.length(), 11) // 1 type byte + 10 varint bytes
  assert_eq(encoded64[0], b'\x40')
  let encoded32 = @spop.encode_data(Int32(-1))
  assert_eq(encoded32.length(), 11)
  assert_eq(encoded32[0], b'\x20')
  // While small non-negative values stay compact.
  assert_eq(@spop.encode_data(Int32(1)), b"\x20\x01")
  assert_eq(@spop.encode_data(UInt64(239)), b"\x50\xef")
  assert_eq(@spop.encode_data(Null), b"\x00")
  assert_eq(@spop.encode_data(Bool(true)), b"\x11")
  assert_eq(@spop.encode_data(Bool(false)), b"\x10")
}

///|
test "frame roundtrip for all six frame types" {
  let frames : Array[@spop.Frame] = [
    @spop.Frame::hello(
      HAProxyHello,
      @spop.hello_args(
        versions=["2.0", "1.9"],
        max_frame_size=16380,
        capabilities=["pipelining"],
        engine_id=Some("haproxy"),
      ),
    ),
    @spop.Frame::hello(
      AgentHello,
      @spop.hello_args(max_frame_size=4096, capabilities=["pipelining"]),
    ),
    @spop.Frame::disconnect(HAProxyDisconnect, Normal, "closing"),
    @spop.Frame::disconnect(AgentDisconnect, UnknownError, "boom"),
    @spop.Frame::notify(123, 456, [
      {
        name: "check-request",
        args: [("ip", Str("1.2.3.4")), ("port", UInt32(8080))],
      },
      { name: "empty-message", args: [], },
    ]),
    @spop.Frame::ack(123, 456, [
      SetVar(Transaction, "txn.block", Bool(true)),
      SetVar(Process, "proc.score", Int32(-5)),
      UnsetVar(Request, "req.unused"),
    ]),
  ]
  for frame in frames {
    assert_eq(@spop.decode_frame(@spop.encode_frame(frame)), Some(frame))
  }
}

///|
test "decode a hand-crafted ACK frame byte-for-byte" {
  // <LEN=13><TYPE=103 ACK><FLAGS=FIN><STREAM-ID=0><FRAME-ID=0>
  // <set-var><3 args><scope=2 txn><name "a"><Bool(true)>
  let bytes : Bytes = [
    0x00, 0x00, 0x00, 0x0D, 0x67, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 0x03,
    0x02, 0x01, 0x61, 0x11,
  ]
  let expected = @spop.Frame::ack(0, 0, [SetVar(Transaction, "a", Bool(true))])
  assert_eq(@spop.decode_frame(bytes), Some(expected))
  // Encoding the frame produces exactly those bytes.
  assert_eq(@spop.encode_frame(expected), bytes)
}

///|
test "disconnect args roundtrip" {
  let args = @spop.disconnect_args(Timeout, "timed out")
  assert_eq(@spop.parse_disconnect_args(args), (Timeout, "timed out"))
  assert_eq(@spop.parse_disconnect_args([]), (UnknownError, ""))
}

///|
test "truncated frame input returns None" {
  let frame = @spop.Frame::ack(1, 1, [UnsetVar(Session, "s.x")])
  let bytes = @spop.encode_frame(frame)
  assert_eq(@spop.decode_frame(b""), None)
  assert_eq(@spop.decode_frame(b"\x00\x00\x00"), None) // incomplete length prefix
  assert_eq(@spop.decode_frame(bytes[:6]), None) // incomplete body
  assert_eq(@spop.decode_frame(bytes[:bytes.length() - 1]), None)
  assert_eq(@spop.decode_frame(bytes), Some(frame))
}

///|
test "frame larger than max-frame-size raises FrameTooBig" {
  // The announced length (0x1001 = 4097) exceeds the limit even though the
  // full frame has not been received yet.
  try @spop.decode_frame(b"\x00\x00\x10\x01", max_frame_size=256) catch {
    @spop.SpopError(code~, ..) => assert_eq(code, FrameTooBig)
  } noraise {
    _ => fail("expected FrameTooBig")
  }
}

///|
test "frame without FIN raises FragmentationNotSupported" {
  let frame = { ..@spop.Frame::ack(0, 0, []), fin: false, }
  try @spop.decode_frame(@spop.encode_frame(frame)) catch {
    @spop.SpopError(code~, ..) => assert_eq(code, FragmentationNotSupported)
  } noraise {
    _ => fail("expected FragmentationNotSupported")
  }
}

///|
test "unknown frame type raises InvalidFrame" {
  // Body: <TYPE=0x37 unknown><FLAGS=FIN><STREAM-ID=0><FRAME-ID=0>
  let bytes : Bytes = [
    0x00, 0x00, 0x00, 0x07, 0x37, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
  ]
  try @spop.decode_frame(bytes) catch {
    @spop.SpopError(code~, ..) => assert_eq(code, InvalidFrame)
  } noraise {
    _ => fail("expected InvalidFrame")
  }
}

///|
test "truncated frame body raises InvalidFrame" {
  // Length prefix announces 7 bytes but the varint frame ids are missing.
  let bytes : Bytes = [0x00, 0x00, 0x00, 0x07, 0x67, 0x00, 0x00, 0x00, 0x01]
  try @spop.decode_frame_body(bytes[4:]) catch {
    @spop.SpopError(code~, ..) => assert_eq(code, InvalidFrame)
  } noraise {
    _ => fail("expected InvalidFrame")
  }
}

///|
test "hello list parsing and version negotiation" {
  assert_eq(@spop.parse_list(" 2.0 , 1.9 ,, 2.1 "), ["2.0", "1.9", "2.1"])
  assert_eq(@spop.parse_list(""), [])
  assert_eq(@spop.choose_version(["1.0", "2.0"]), "2.0")
  // Announcing a major version implies support of all its minor versions.
  assert_eq(@spop.choose_version(["2.3"]), "2.0")
  try @spop.choose_version(["1.0", "3.0"]) catch {
    @spop.SpopError(code~, ..) => assert_eq(code, UnsupportedVersion)
  } noraise {
    _ => fail("expected UnsupportedVersion")
  }
}

///|
test "max-frame-size negotiation" {
  assert_eq(@spop.negotiate_max_frame_size(16380, 65536), 16380)
  assert_eq(@spop.negotiate_max_frame_size(65536, 16380), 16380)
  assert_eq(@spop.negotiate_max_frame_size(256, 256), 256)
  try @spop.negotiate_max_frame_size(16380, 100) catch {
    @spop.SpopError(code~, ..) => assert_eq(code, InvalidMaxFrameSize)
  } noraise {
    _ => fail("expected InvalidMaxFrameSize")
  }
}

///|
test "hello args roundtrip" {
  let info = @spop.parse_hello_args(
    @spop.hello_args(
      max_frame_size=4096,
      capabilities=["pipelining"],
      healthcheck=true,
      engine_id=Some("engine-1"),
    ),
  )
  assert_eq(info.versions, ["2.0"])
  assert_eq(info.max_frame_size, 4096)
  assert_eq(info.capabilities, ["pipelining"])
  assert_eq(info.healthcheck, true)
  assert_eq(info.engine_id, Some("engine-1"))
}

///|
test "hello args with missing keys raise the matching status code" {
  try @spop.parse_hello_args([]) catch {
    @spop.SpopError(code~, ..) => assert_eq(code, VersionNotFound)
  } noraise {
    _ => fail("expected VersionNotFound")
  }
  try @spop.parse_hello_args([("supported-versions", Str("2.0"))]) catch {
    @spop.SpopError(code~, ..) => assert_eq(code, MaxFrameSizeNotFound)
  } noraise {
    _ => fail("expected MaxFrameSizeNotFound")
  }
  try
    @spop.parse_hello_args([
      ("supported-versions", Str("2.0")),
      ("max-frame-size", UInt32(16380)),
    ])
  catch {
    @spop.SpopError(code~, ..) => assert_eq(code, CapabilitiesNotFound)
  } noraise {
    _ => fail("expected CapabilitiesNotFound")
  }
}
