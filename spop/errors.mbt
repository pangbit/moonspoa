///|
/// Protocol-level error of the SPOP protocol.
///
/// Carries a `StatusCode` that can be reported to the peer inside a
/// DISCONNECT frame, plus a human-readable message.
pub suberror SpopError {
  SpopError(code~ : StatusCode, message~ : String)
}

///|
pub impl Show for SpopError with fn output(self : SpopError, logger : &Logger) -> Unit {
  let SpopError(code~, message~) = self
  logger <+ "SpopError(code=\{Repr(code)}, message=\{message})"
}

///|
/// Create a `SpopError` from a status code and a message.
pub fn SpopError::new(code : StatusCode, message : String) -> SpopError {
  SpopError(code~, message~)
}

///|
/// The status code carried by this error.
pub fn SpopError::code(self : SpopError) -> StatusCode {
  let SpopError(code~, ..) = self
  code
}

///|
/// The human-readable message carried by this error.
pub fn SpopError::message(self : SpopError) -> String {
  let SpopError(message~, ..) = self
  message
}
