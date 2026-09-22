///|
/// Black-box tests for the agent session state machine, driving it with
/// hand-crafted frames over an in-memory duplex (`@io.pipe`).

///|
/// Read one frame from a raw pipe reader.
async fn read_frame(reader : &@io.Reader) -> @spop.Frame {
  let header = reader.read_exactly(4)
  guard @spop.decode_frame_length(header) is Some(len) else {
    fail("invalid frame length prefix")
  }
  @spop.decode_frame_body(reader.read_exactly(len.reinterpret_as_int()))
}

///|
/// Run an agent session on one end of an in-memory duplex and invoke `f`
/// with the peer (reader, writer) end. The agent-side pipe ends are closed
/// once the session returns so the peer observes EOF.
async fn with_agent(
  agent : @agent.Agent,
  f : async (&@io.Reader, &@io.Writer) -> Unit,
) -> Unit {
  let (agent_reader, client_writer) = @io.pipe()
  let (client_reader, agent_writer) = @io.pipe()
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      @agent.Session::new(agent).run(agent_reader, agent_writer)
      agent_writer.close()
      agent_reader.close()
    })
    f(client_reader, client_writer)
  })
}

///|
/// Assert that `frame` is an AGENT-DISCONNECT with status `expected`.
fn expect_disconnect(
  frame : @spop.Frame,
  expected : @spop.StatusCode,
) -> Unit raise {
  assert_eq(frame.frame_type, AgentDisconnect)
  guard frame.payload is KeyValues(args) else {
    fail("expected key-value payload in AGENT-DISCONNECT")
  }
  let (code, _) = @spop.parse_disconnect_args(args)
  assert_eq(code, expected)
}

///|
fn agent_hello_args(
  capabilities : Array[String],
) -> Array[(String, @spop.Data)] {
  @spop.hello_args(max_frame_size=1024, capabilities~)
}

///|
test "Agent::new rejects too small max-frame-size" {
  try @agent.Agent::new(max_frame_size=100) catch {
    @spop.SpopError(code~, ..) => assert_eq(code, InvalidMaxFrameSize)
  } noraise {
    _ => fail("expected InvalidMaxFrameSize")
  }
}

///|
async test "first frame must be haproxy-hello" {
  with_agent(@agent.Agent::new(), (reader, writer) => {
    writer.write(@spop.encode_frame(@spop.Frame::notify(0, 1, [])))
    expect_disconnect(read_frame(reader), InvalidFrame)
  })
}

///|
async test "oversized frame reports frame-too-big" {
  with_agent(@agent.Agent::new(max_frame_size=256), (reader, writer) => {
    writer.write(
      @spop.encode_frame(@spop.Frame::hello(HAProxyHello, agent_hello_args([]))),
    )
    assert_eq(read_frame(reader).frame_type, AgentHello)
    // Announce a frame of 257 bytes, over the agent's 256-byte limit.
    writer.write(b"\x00\x00\x01\x01")
    expect_disconnect(read_frame(reader), FrameTooBig)
  })
}

///|
async test "sequential notifies are answered in order" {
  let agent = @agent.Agent::new(capabilities=[]).on("m", fn(msg) {
    [SetVar(Transaction, "txn.seen", Str(msg.name))]
  })
  with_agent(agent, (reader, writer) => {
    writer.write(
      @spop.encode_frame(@spop.Frame::hello(HAProxyHello, agent_hello_args([]))),
    )
    assert_eq(read_frame(reader).frame_type, AgentHello)
    // The pipe is rendezvous-style: a write blocks until the agent has
    // read the whole frame, and the sequential agent answers each NOTIFY
    // before reading the next one, so writes and reads must alternate.
    writer.write(
      @spop.encode_frame(@spop.Frame::notify(0, 1, [{ name: "m", args: [], }])),
    )
    let ack1 = read_frame(reader)
    writer.write(
      @spop.encode_frame(
        @spop.Frame::notify(0, 2, [{ name: "unknown", args: [], }]),
      ),
    )
    let ack2 = read_frame(reader)
    assert_eq((ack1.frame_type, ack1.frame_id), (Ack, 1))
    assert_eq((ack2.frame_type, ack2.frame_id), (Ack, 2))
    guard ack1.payload is Actions(actions1) else { fail("expected actions") }
    assert_eq(actions1, [SetVar(Transaction, "txn.seen", Str("m"))])
    guard ack2.payload is Actions(actions2) else { fail("expected actions") }
    assert_eq(actions2, [])
    writer.write(
      @spop.encode_frame(@spop.Frame::disconnect(HAProxyDisconnect, Normal, "")),
    )
    expect_disconnect(read_frame(reader), Normal)
  })
}

///|
async test "pipelined notifies are answered concurrently" {
  let (agent_reader, client_writer) = @io.pipe()
  let (client_reader, agent_writer) = @io.pipe()
  let agent = @agent.Agent::new().on("m", fn(msg) {
    [SetVar(Session, "sess.v", Str(msg.name))]
  })
  let session = @agent.Session::new(agent)
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      session.run(agent_reader, agent_writer)
      agent_writer.close()
      agent_reader.close()
    })
    client_writer.write(
      @spop.encode_frame(
        @spop.Frame::hello(HAProxyHello, agent_hello_args(["pipelining"])),
      ),
    )
    assert_eq(read_frame(client_reader).frame_type, AgentHello)
    assert_true(session.pipelining())
    assert_eq(session.negotiated_max_frame_size(), 1024)
    client_writer.write(
      @spop.encode_frame(@spop.Frame::notify(0, 1, [{ name: "m", args: [], }])),
    )
    client_writer.write(
      @spop.encode_frame(@spop.Frame::notify(0, 2, [{ name: "m", args: [], }])),
    )
    let ack1 = read_frame(client_reader)
    let ack2 = read_frame(client_reader)
    // ACKs may arrive in any order; both must be present and correct.
    let ids = [ack1.frame_id, ack2.frame_id]
    ids.sort()
    assert_eq(ids, [1, 2])
    for ack in [ack1, ack2] {
      assert_eq(ack.frame_type, Ack)
      guard ack.payload is Actions(actions) else { fail("expected actions") }
      assert_eq(actions, [SetVar(Session, "sess.v", Str("m"))])
    }
    client_writer.write(
      @spop.encode_frame(
        @spop.Frame::disconnect(HAProxyDisconnect, Normal, "done"),
      ),
    )
    expect_disconnect(read_frame(client_reader), Normal)
  })
  assert_eq(session.peer_disconnect(), Some((Normal, "done")))
}

///|
async test "agent-hello announces the negotiated max-frame-size" {
  // The peer announces 1024, below the agent default of 16380; the
  // AGENT-HELLO must carry the lower value (SPOE.txt 3.2.4).
  with_agent(@agent.Agent::new(), (reader, writer) => {
    writer.write(
      @spop.encode_frame(@spop.Frame::hello(HAProxyHello, agent_hello_args([]))),
    )
    let hello = read_frame(reader)
    assert_eq(hello.frame_type, AgentHello)
    guard hello.payload is KeyValues(args) else { fail("expected key-values") }
    let mut max_frame_size : UInt = 0
    for kv in args {
      if kv is ("max-frame-size", UInt32(v)) {
        max_frame_size = v
      }
    }
    assert_eq(max_frame_size, 1024)
    writer.write(
      @spop.encode_frame(@spop.Frame::disconnect(HAProxyDisconnect, Normal, "")),
    )
    expect_disconnect(read_frame(reader), Normal)
  })
}

///|
async test "aborted notify gets an empty ack without invoking handlers" {
  let calls : Ref[Int] = Ref(0)
  let agent = @agent.Agent::new().on("m", fn(_) {
    calls.val += 1
    [SetVar(Process, "proc.x", Null)]
  })
  with_agent(agent, (reader, writer) => {
    writer.write(
      @spop.encode_frame(@spop.Frame::hello(HAProxyHello, agent_hello_args([]))),
    )
    assert_eq(read_frame(reader).frame_type, AgentHello)
    let aborted = {
      ..@spop.Frame::notify(0, 9, [{ name: "m", args: [], }]),
      abort: true,
    }
    writer.write(@spop.encode_frame(aborted))
    let ack = read_frame(reader)
    assert_eq((ack.frame_type, ack.frame_id), (Ack, 9))
    guard ack.payload is Actions(actions) else { fail("expected actions") }
    assert_eq(actions, [])
    assert_eq(calls.val, 0)
    writer.write(
      @spop.encode_frame(@spop.Frame::disconnect(HAProxyDisconnect, Normal, "")),
    )
    expect_disconnect(read_frame(reader), Normal)
  })
}
