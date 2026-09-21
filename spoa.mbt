///|
/// Re-export the SPOP protocol core types so that users can work with a
/// single `@spoa` import.
pub using @spop {
  type Data,
  type VarScope,
  type Action,
  type Message,
  type Frame,
  type FrameType,
  type FramePayload,
  type StatusCode,
  type HelloInfo,
  type SpopError,
}

///|
/// Re-export the agent-side configuration and session types.
pub using @agent {type Agent, type Session}
