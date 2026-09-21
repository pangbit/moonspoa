///|
/// Helpers for the HELLO handshake negotiation of the SPOP protocol.

///|
/// The SPOP protocol version implemented by this library.
pub let supported_version : String = "2.0"

///|
/// Minimum acceptable max-frame-size (256 bytes, per the SPOE spec).
pub let min_frame_size : UInt = 256

///|
/// Parse a comma-separated list such as the values of the
/// "supported-versions" and "capabilities" HELLO keys. ASCII whitespace
/// around items is ignored and empty items are dropped.
pub fn parse_list(s : String) -> Array[String] {
  let items = []
  for part in s.split(",") {
    let item = part.trim().to_owned()
    if item != "" {
      items.push(item)
    }
  }
  items
}

///|
/// The major version of a "Major.Minor" version string; `None` if the
/// string does not start with a decimal number.
fn major_version(version : String) -> Int? {
  let mut major = 0
  let mut seen_digit = false
  for c in version {
    if c == '.' {
      break
    }
    if c is ('0'..='9') {
      major = major * 10 + (c.to_int() - '0'.to_int())
      seen_digit = true
    } else {
      return None
    }
  }
  if seen_digit {
    Some(major)
  } else {
    None
  }
}

///|
/// Choose the protocol version to use given the versions announced by the
/// peer in its "supported-versions" HELLO key. Versions have the form
/// "Major.Minor"; announcing a major version implies support for all its
/// minor versions. This library supports version "2.0", so any announced
/// version with major version 2 matches. Raises
/// `StatusCode::UnsupportedVersion` when there is no common version.
pub fn choose_version(announced : Array[String]) -> String raise SpopError {
  for version in announced {
    if major_version(version) is Some(2) {
      return supported_version
    }
  }
  raise SpopError::new(
    UnsupportedVersion,
    "no common SPOP version; peer announced: \{announced.join(", ")}",
  )
}

///|
/// Compute the effective max-frame-size from our own and the peer's
/// announced value: the minimum of both. Raises
/// `StatusCode::InvalidMaxFrameSize` if the result is smaller than 256.
pub fn negotiate_max_frame_size(
  ours : UInt,
  theirs : UInt,
) -> UInt raise SpopError {
  let effective = if ours < theirs { ours } else { theirs }
  if effective < min_frame_size {
    raise SpopError::new(
      InvalidMaxFrameSize,
      "max-frame-size \{effective} is smaller than the minimum \{min_frame_size}",
    )
  }
  effective
}

///|
/// The parameters extracted from a HELLO frame (either direction).
pub(all) struct HelloInfo {
  versions : Array[String]
  max_frame_size : UInt
  capabilities : Array[String]
  healthcheck : Bool
  engine_id : String?
} derive(Eq, Debug)

///|
/// Parse the key-value list of a HELLO frame. Missing required keys raise
/// the corresponding status code (`VersionNotFound`,
/// `MaxFrameSizeNotFound`, `CapabilitiesNotFound`); keys of the wrong type
/// raise `InvalidFrame`. Unknown keys are ignored.
pub fn parse_hello_args(
  args : Array[(String, Data)],
) -> HelloInfo raise SpopError {
  let mut versions : Array[String]? = None
  let mut max_frame_size : UInt? = None
  let mut capabilities : Array[String]? = None
  let mut healthcheck = false
  let mut engine_id : String? = None
  for kv in args {
    match kv {
      ("supported-versions", Str(s)) => versions = Some(parse_list(s))
      ("supported-versions", _) =>
        raise SpopError::new(
          InvalidFrame,
          "supported-versions must be a STRING",
        )
      ("max-frame-size", UInt32(v)) => max_frame_size = Some(v)
      ("max-frame-size", _) =>
        raise SpopError::new(InvalidFrame, "max-frame-size must be a UINT32")
      ("capabilities", Str(s)) => capabilities = Some(parse_list(s))
      ("capabilities", _) =>
        raise SpopError::new(InvalidFrame, "capabilities must be a STRING")
      ("healthcheck", Bool(v)) => healthcheck = v
      ("engine-id", Str(s)) => engine_id = Some(s)
      _ => ()
    }
  }
  guard versions is Some(versions) else {
    raise SpopError::new(
      VersionNotFound,
      "missing supported-versions key in HELLO frame",
    )
  }
  guard max_frame_size is Some(max_frame_size) else {
    raise SpopError::new(
      MaxFrameSizeNotFound,
      "missing max-frame-size key in HELLO frame",
    )
  }
  guard capabilities is Some(capabilities) else {
    raise SpopError::new(
      CapabilitiesNotFound,
      "missing capabilities key in HELLO frame",
    )
  }
  { versions, max_frame_size, capabilities, healthcheck, engine_id, }
}

///|
/// Build the key-value list of a HELLO frame announcing `versions`,
/// `max_frame_size` and `capabilities`, plus optional `healthcheck` and
/// `engine_id`.
pub fn hello_args(
  versions? : Array[String] = [supported_version],
  max_frame_size? : UInt = default_max_frame_size,
  capabilities? : Array[String] = [],
  healthcheck? : Bool = false,
  engine_id? : String? = None,
) -> Array[(String, Data)] {
  let args : Array[(String, Data)] = [
    ("supported-versions", Str(versions.join(", "))),
    ("max-frame-size", UInt32(max_frame_size)),
    ("capabilities", Str(capabilities.join(", "))),
  ]
  if healthcheck {
    args.push(("healthcheck", Bool(true)))
  }
  if engine_id is Some(id) {
    args.push(("engine-id", Str(id)))
  }
  args
}
