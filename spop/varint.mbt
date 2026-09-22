///|
/// Variable-length integer encoding used by the SPOP protocol.
///
/// The algorithm follows haproxy's `encode_varint`/`decode_varint`
/// (`include/haproxy/intops.h`) exactly:
///
/// - encode: values `< 240` are written as a single byte. Otherwise the
///   first byte is `0xF0 | (i & 0x0F)`, `i = (i - 240) >> 4`, then while
///   `i >= 128` a byte `0x80 | (i & 0x7F)` is written and
///   `i = (i - 128) >> 7`; the final byte is `i`.
/// - decode: if the first byte is `< 240` it is the value. Otherwise
///   `v = b0`, `shift = 4`, and for each following byte `b`:
///   `v += b << shift; shift += 7`, stopping at the first `b < 128`
///   (the high bit of `b` is NOT masked off, mirroring the `- 128` of the
///   encoder). A byte that would make the accumulated value exceed 64
///   bits is rejected as an overflow.

///|
/// Encode `i` as a SPOP varint.
pub fn encode_varint(i : UInt64) -> Bytes {
  let buf = Buffer(size_hint=10)
  write_varint(buf, i)
  buf.to_bytes()
}

///|
/// Append the varint encoding of `i` to `buf`.
fn write_varint(buf : Buffer, i : UInt64) -> Unit {
  if i < 240 {
    buf.write_byte(i.to_byte())
    return
  }
  let mut i = i
  buf.write_byte((0xF0UL | (i & 0x0FUL)).to_byte())
  i = (i - 240) >> 4
  while i >= 128 {
    buf.write_byte((0x80UL | (i & 0x7FUL)).to_byte())
    i = (i - 128) >> 7
  }
  buf.write_byte(i.to_byte())
}

///|
/// Decode a SPOP varint from the start of `bytes`.
///
/// Returns the decoded value and the number of bytes consumed, or `None`
/// if `bytes` does not yet contain a complete varint (the caller should
/// read more data and retry). Raises `SpopError` with
/// `StatusCode::InvalidFrame` if the varint is too long to fit in 64 bits
/// or the accumulated value would overflow past bit 63.
pub fn decode_varint(bytes : BytesView) -> (UInt64, Int)? raise SpopError {
  if bytes.length() == 0 {
    return None
  }
  let b0 = bytes[0].to_uint64()
  if b0 < 240 {
    return Some((b0, 1))
  }
  let mut v = b0
  let mut shift = 4
  for i = 1; ; i = i + 1 {
    if i >= bytes.length() {
      return None
    }
    if shift >= 64 {
      raise SpopError::new(InvalidFrame, "varint too long")
    }
    let b = bytes[i].to_uint64()
    // The addition itself must not wrap: v + (b << shift) fits in 64
    // bits iff b <= (UInt64::max - v) >> shift. This also rejects bytes
    // whose own value bits exceed the remaining capacity, since
    // (UInt64::max - v) >> shift < 1 << (64 - shift).
    if b > (0xFFFF_FFFF_FFFF_FFFFUL - v) >> shift {
      raise SpopError::new(InvalidFrame, "varint overflows 64 bits")
    }
    v += b << shift
    shift += 7
    if b < 128 {
      return Some((v, i + 1))
    }
  }
}
