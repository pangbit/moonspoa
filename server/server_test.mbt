///|
/// Integration tests for the server transports: real TCP loopback and a
/// real Unix domain socket, with `client` as the peer driver.

///|
/// An agent that sets `txn.ip` from the "check-ip" message's `ip` argument.
fn test_agent() -> @agent.Agent raise {
  @agent.Agent::new().on("check-ip", fn(msg) {
    guard msg.args is [("ip", Str(ip))] else { return [] }
    [SetVar(Transaction, "txn.ip", Str(ip)), UnsetVar(Request, "req.dummy")]
  })
}

///|
async test "tcp server full flow" {
  let server = @server.Server::bind(
    test_agent(),
    @server.Listener::tcp("127.0.0.1:0"),
  )
  @async.with_task_group(group => {
    let server_task = group.spawn(() => server.run())
    let addr = server.addr().unwrap()
    let conn = @socket.Tcp::connect(addr)
    defer conn.close()
    let client = @client.Client::hello(conn, conn)
    assert_eq(client.version(), "2.0")
    let (sid, fid) = client.next_ids()
    let actions = client.notify(sid, fid, [
      { name: "check-ip", args: [("ip", Str("192.168.1.1"))], },
    ])
    assert_eq(actions, [
      SetVar(Transaction, "txn.ip", Str("192.168.1.1")),
      UnsetVar(Request, "req.dummy"),
    ])
    let (code, _) = client.disconnect()
    assert_eq(code, Normal)
    server_task.cancel()
    server.close()
  })
}

///|
async test "uds server full flow and socket file cleanup" {
  let path = "/tmp/spoa-test-\{@async.now()}.sock"
  let server = @server.Server::bind(test_agent(), Unix(path))
  @async.with_task_group(group => {
    let server_task = group.spawn(() => server.run())
    let stream = @server.connect_unix(path)
    let client = @client.Client::hello(stream, stream)
    let (sid, fid) = client.next_ids()
    let actions = client.notify(sid, fid, [
      { name: "check-ip", args: [("ip", Str("10.1.2.3"))], },
    ])
    assert_eq(actions, [
      SetVar(Transaction, "txn.ip", Str("10.1.2.3")),
      UnsetVar(Request, "req.dummy"),
    ])
    ignore(client.disconnect())
    stream.close()
    server_task.cancel()
    server.close()
  })
  // Server::close removed the socket file.
  assert_false(@fs.exists(path))
}

///|
async test "uds connect to a missing socket fails" {
  let path = "/tmp/spoa-test-nonexistent-\{@async.now()}.sock"
  try @server.connect_unix(path) catch {
    @server.UdsConnect(..) => ()
    err => fail("unexpected error: \{err}")
  } noraise {
    _ => fail("expected UdsConnect")
  }
}

///|
async test "uds listener removes a stale socket file on bind" {
  let path = "/tmp/spoa-test-stale-\{@async.now()}.sock"
  let server1 = @server.Server::bind(test_agent(), Unix(path))
  assert_true(@fs.exists(path))
  // Binding the same path again unlinks the stale file first.
  let server2 = @server.Server::bind(test_agent(), Unix(path))
  server2.close()
  assert_false(@fs.exists(path))
  server1.close()
}
