///|
/// Black-box tests for the SPOE client, connected to a real agent session
/// over an in-memory duplex (two cross-connected `@io.pipe` pairs).

///|
/// Run an agent session on one end of an in-memory duplex and invoke `f`
/// with the client (reader, writer) end.
async fn with_session(
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
async test "hello notify disconnect flow" {
  let agent = @agent.Agent::new().on("check-ip", fn(msg) {
    guard msg.args is [("ip", Str(ip))] else { return [] }
    [SetVar(Transaction, "txn.ip", Str(ip)), UnsetVar(Request, "req.dummy")]
  })
  with_session(agent, (reader, writer) => {
    let client = @client.Client::hello(reader, writer)
    assert_eq(client.version(), "2.0")
    assert_true(client.pipelining())
    assert_eq(client.peer_max_frame_size(), @spop.default_max_frame_size)
    let (sid, fid) = client.next_ids()
    let actions = client.notify(sid, fid, [
      { name: "check-ip", args: [("ip", Str("10.0.0.1"))], },
    ])
    assert_eq(actions, [
      SetVar(Transaction, "txn.ip", Str("10.0.0.1")),
      UnsetVar(Request, "req.dummy"),
    ])
    let (code, _) = client.disconnect()
    assert_eq(code, Normal)
  })
}

///|
async test "healthcheck hello is closed by the agent right away" {
  with_session(@agent.Agent::new(), (reader, writer) => {
    ignore(@client.Client::hello(reader, writer, healthcheck=true))
    // The agent answered AGENT-HELLO and hung up: the next read fails
    // (ReaderClosed on EOF).
    try reader.read_exactly(1) catch {
      @io.ReaderClosed => ()
      _ => ()
    } noraise {
      _ => fail("expected the connection to be closed")
    }
  })
}

///|
async test "incompatible version raises unsupported-version" {
  with_session(@agent.Agent::new(), (reader, writer) => {
    try
      @client.Client::hello(reader, writer, supported_versions=["3.0"])
    catch {
      @spop.SpopError(code~, ..) => assert_eq(code, UnsupportedVersion)
      err => fail("unexpected error: \{err}")
    } noraise {
      _ => fail("expected UnsupportedVersion")
    }
  })
}

///|
async test "unknown message names get an empty ack" {
  with_session(@agent.Agent::new(), (reader, writer) => {
    let client = @client.Client::hello(reader, writer)
    let (sid, fid) = client.next_ids()
    assert_eq(client.notify(sid, fid, [{ name: "nope", args: [], }]), [])
    ignore(client.disconnect())
  })
}

///|
priv suberror Boom {
  Boom
}

///|
async test "handler errors are reported and skipped, session survives" {
  let errors : Array[String] = []
  let agent = @agent.Agent::new(on_error=fn(name, _err) { errors.push(name) })
    .on("boom", _ => raise Boom)
    .on("ok", fn(_) { [SetVar(Process, "proc.ok", Bool(true))] })
  with_session(agent, (reader, writer) => {
    let client = @client.Client::hello(reader, writer)
    let (sid1, fid1) = client.next_ids()
    assert_eq(client.notify(sid1, fid1, [{ name: "boom", args: [], }]), [])
    assert_eq(errors, ["boom"])
    // The session is still alive: the next notify works normally.
    let (sid2, fid2) = client.next_ids()
    assert_eq(client.notify(sid2, fid2, [{ name: "ok", args: [], }]), [
      SetVar(Process, "proc.ok", Bool(true)),
    ])
    ignore(client.disconnect())
  })
}

///|
async test "sequential notifies with increasing ids" {
  let agent = @agent.Agent::new().on("count", fn(msg) {
    [SetVar(Transaction, "txn.name", Str(msg.name))]
  })
  with_session(agent, (reader, writer) => {
    // No pipelining announced: the agent answers sequentially.
    let client = @client.Client::hello(reader, writer, capabilities=[])
    assert_false(client.pipelining())
    for i in 1..<=3 {
      let (sid, fid) = client.next_ids()
      assert_eq((sid, fid), (0, i.to_uint64()))
      let actions = client.notify(sid, fid, [{ name: "count", args: [], }])
      assert_eq(actions, [SetVar(Transaction, "txn.name", Str("count"))])
    }
    ignore(client.disconnect())
  })
}
