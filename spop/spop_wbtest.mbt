///|
/// White-box tests for the SPOP varint codec, byte-for-byte against the
/// haproxy `encode_varint`/`decode_varint` algorithm (intops.h).

///|
/// Boundary values of the varint encoding with their exact wire bytes.
let varint_vectors : Array[(UInt64, Bytes)] = [
  (0, [0x00]),
  (239, [0xEF]),
  (240, [0xF0, 0x00]),
  (241, [0xF1, 0x00]),
  (2287, [0xFF, 0x7F]),
  (2288, [0xF0, 0x80, 0x00]),
  (264431, [0xFF, 0xFF, 0x7F]),
  (264432, [0xF0, 0x80, 0x80, 0x00]),
  (33818863, [0xFF, 0xFF, 0xFF, 0x7F]),
  (33818864, [0xF0, 0x80, 0x80, 0x80, 0x00]),
  (4328786159, [0xFF, 0xFF, 0xFF, 0xFF, 0x7F]),
  (4328786160, [0xF0, 0x80, 0x80, 0x80, 0x80, 0x00]),
]

///|
test "varint encode boundary vectors" {
  for case in varint_vectors {
    let (value, expected) = case
    assert_eq(encode_varint(value), expected)
  }
  // UInt64::max is the largest encodable value: 10 bytes.
  assert_eq(encode_varint(18446744073709551615).length(), 10)
}

///|
test "varint decode boundary vectors" {
  for case in varint_vectors {
    let (value, encoded) = case
    assert_eq(decode_varint(encoded), Some((value, encoded.length())))
  }
}

///|
test "varint encode/decode roundtrip" {
  let values : Array[UInt64] = [
    0, 1, 127, 128, 239, 240, 241, 255, 256, 2287, 2288, 264431, 264432, 33818863,
    33818864, 4328786159, 4328786160, 18446744073709551615,
  ]
  for v in values {
    let encoded = encode_varint(v)
    assert_eq(decode_varint(encoded), Some((v, encoded.length())))
  }
}

///|
test "varint decode reports consumed byte count" {
  assert_eq(decode_varint([0xEF, 0xAA, 0xBB]), Some((239, 1)))
  assert_eq(decode_varint([0xF1, 0x00, 0xAA]), Some((241, 2)))
}

///|
test "varint decode with insufficient input returns None" {
  assert_eq(decode_varint(b""), None)
  assert_eq(decode_varint([0xF0]), None)
  assert_eq(decode_varint([0xF0, 0x80]), None)
  assert_eq(decode_varint([0xF0, 0x80, 0x80]), None)
}

///|
test "varint decode rejects overflow past 64 bits" {
  // At byte index 9 the shift is 60, so only 4 bits of capacity remain;
  // any byte >= 16 overflows instead of wrapping.
  let overflowing : Array[Bytes] = [
    // The audit vector: 9 x 0xFF then 0x7F used to wrap around.
    [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F],
    // One bit too many on the final byte.
    [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x10],
    // Continuation bit set where no further byte fits.
    [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x8F],
  ]
  for bytes in overflowing {
    try decode_varint(bytes) catch {
      SpopError(code~, ..) => assert_eq(code, InvalidFrame)
    } noraise {
      _ => fail("expected InvalidFrame for overflowing varint")
    }
  }
  // The largest legal 10-byte varint (UInt64::max) still decodes: its
  // final byte is 15, exactly filling the remaining 4 bits.
  let max = encode_varint(18446744073709551615)
  assert_eq(max.length(), 10)
  assert_eq(decode_varint(max), Some((18446744073709551615, 10)))
}
