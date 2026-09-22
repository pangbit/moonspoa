///|
/// Concurrency stress test: many engine connections in parallel, each
/// sending a burst of pipelined NOTIFY frames (all written before any ACK
/// is read). Every ACK must echo its NOTIFY's (stream-id, frame-id) and
/// carry the actions for that NOTIFY's arguments.

///|
/// Read one frame from a raw connection.
async fn read_frame(reader : &@io.Reader) -> @spop.Frame {
  let header = reader.read_exactly(4)
  guard @spop.decode_frame_length(header) is Some(len) else {
    fail("invalid frame length prefix")
  }
  @spop.decode_frame_body(reader.read_exactly(len.reinterpret_as_int()))
}

///|
/// One simulated engine: HELLO, then `count` pipelined NOTIFYs on distinct
/// stream ids, then verify the `count` ACKs (any order) by echoed stream id
/// and by the per-message `ip` value round-tripped through the action.
async fn engine(addr : @socket.Addr, engine_id : Int, count : Int) -> Unit {
  fn ip_of(i : Int) -> String {
    "10.\{engine_id / 256}.\{engine_id % 256}.\{i}"
  }

  fn sid_of(i : Int) -> UInt64 {
    (engine_id * 1000 + i).to_uint64()
  }

  let conn = @socket.Tcp::connect(addr)
  defer conn.close()
  conn.write(
    @spop.encode_frame(
      @spop.Frame::hello(
        HAProxyHello,
        @spop.hello_args(capabilities=["pipelining"]),
      ),
    ),
  )
  assert_eq(read_frame(conn).frame_type, AgentHello)
  for i in 0..<count {
    conn.write(
      @spop.encode_frame(
        @spop.Frame::notify(sid_of(i), 0, [
          { name: "check-ip", args: [("ip", Str(ip_of(i)))], },
        ]),
      ),
    )
  }
  let seen : Map[UInt64, Bool] = Map([])
  for _ in 0..<count {
    let ack = read_frame(conn)
    assert_eq(ack.frame_type, Ack)
    assert_eq(ack.frame_id, 0)
    let i = (ack.stream_id - sid_of(0)).reinterpret_as_int64().to_int()
    guard ack.payload is Actions([SetVar(Transaction, "txn.ip", Str(ip)), ..]) else {
      fail("unexpected ACK payload for stream \{ack.stream_id}")
    }
    assert_eq(ip, ip_of(i))
    assert_false(seen.contains(ack.stream_id))
    seen[ack.stream_id] = true
  }
  assert_eq(seen.length(), count)
  conn.write(
    @spop.encode_frame(@spop.Frame::disconnect(HAProxyDisconnect, Normal, "")),
  )
  assert_eq(read_frame(conn).frame_type, AgentDisconnect)
}

///|
async test "concurrent pipelined engines over real TCP" {
  let server = @server.Server::bind(
    test_agent(),
    @server.Listener::tcp("127.0.0.1:0"),
  )
  @async.with_task_group(group => {
    let server_task = group.spawn(() => server.run())
    let addr = server.addr().unwrap()
    let handles = []
    for e in 0..<32 {
      handles.push(group.spawn(() => engine(addr, e, 50)))
    }
    for handle in handles {
      handle.wait()
    }
    server_task.cancel()
    server.close()
  })
}
