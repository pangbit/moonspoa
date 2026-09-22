///|
/// Black-box tests for the pipelined mode of the SPOE client
/// (`Client::with_pipelining`): concurrent in-flight NOTIFY frames, ACK
/// dispatch by (stream-id, frame-id), out-of-order ACKs, terminal error
/// propagation, send-side frame size enforcement and notify timeouts.

///|
/// Read one frame from a raw connection (scripted-peer tests).
async fn read_raw_frame(reader : &@io.Reader) -> @spop.Frame {
  let header = reader.read_exactly(4)
  guard @spop.decode_frame_length(header) is Some(len) else {
    fail("invalid frame length prefix")
  }
  @spop.decode_frame_body(reader.read_exactly(len.reinterpret_as_int()))
}

///|
/// The agent side of the HELLO handshake for scripted peers: read the
/// HAPROXY-HELLO frame and reply with an AGENT-HELLO announcing
/// `capabilities`.
async fn scripted_hello(
  reader : &@io.Reader,
  writer : &@io.Writer,
  capabilities? : Array[String] = ["pipelining"],
) -> Unit {
  let hello = read_raw_frame(reader)
  assert_eq(hello.frame_type, HAProxyHello)
  writer.write(
    @spop.encode_frame(
      @spop.Frame::hello(AgentHello, [
        ("version", Str("2.0")),
        ("max-frame-size", UInt32(@spop.default_max_frame_size)),
        ("capabilities", Str(capabilities.join(", "))),
      ]),
    ),
  )
}

///|
async test "concurrent pipelined notify dispatches acks to their callers" {
  let agent = @agent.Agent::new().on("check-ip", fn(msg) {
    guard msg.args is [("ip", Str(ip))] else { return [] }
    [SetVar(Transaction, "txn.ip", Str(ip))]
  })
  with_session(agent, (reader, writer) => {
    let client = @client.Client::hello(reader, writer)
    assert_true(client.pipelining())
    client.with_pipelining(client => {
      @async.with_task_group(group => {
        for t in 0..<64 {
          group.spawn_bg(() => {
            for i in 0..<10 {
              let ip = "10.0.\{t}.\{i}"
              let actions = client.notify_messages([
                { name: "check-ip", args: [("ip", Str(ip))], },
              ])
              assert_eq(actions, [SetVar(Transaction, "txn.ip", Str(ip))])
            }
          })
        }
      })
    })
    // Back in sequential mode, disconnect works as before.
    let (code, _) = client.disconnect()
    assert_eq(code, Normal)
  })
}

///|
async test "out-of-order acks are dispatched by stream and frame id" {
  let (agent_reader, client_writer) = @io.pipe()
  let (client_reader, agent_writer) = @io.pipe()
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      scripted_hello(agent_reader, agent_writer)
      // Collect 5 NOTIFY frames, then ACK them in reverse order. The ACK
      // carries the echoed frame id so the caller can verify the match.
      let frames : Array[@spop.Frame] = []
      for _ in 0..<5 {
        frames.push(read_raw_frame(agent_reader))
      }
      frames.rev_in_place()
      for frame in frames {
        agent_writer.write(
          @spop.encode_frame(
            @spop.Frame::ack(frame.stream_id, frame.frame_id, [
              SetVar(Process, "proc.fid", UInt64(frame.frame_id)),
            ]),
          ),
        )
      }
    })
    let client = @client.Client::hello(client_reader, client_writer)
    client.with_pipelining(client => {
      @async.with_task_group(group => {
        for _ in 0..<5 {
          group.spawn_bg(() => {
            let (sid, fid) = client.next_ids()
            let actions = client.notify(sid, fid, [{ name: "m", args: [], }])
            guard actions is [SetVar(Process, "proc.fid", UInt64(echoed))] else {
              fail("unexpected actions: \{Repr(actions)}")
            }
            assert_eq(echoed, fid)
          })
        }
      })
    })
  })
}

///|
async test "pipelining not negotiated: concurrent notify is serialized" {
  let agent = @agent.Agent::new().on("m", fn(msg) {
    guard msg.args is [("n", UInt64(n))] else { return [] }
    [SetVar(Process, "proc.n", UInt64(n))]
  })
  with_session(agent, (reader, writer) => {
    let client = @client.Client::hello(reader, writer, capabilities=[])
    assert_false(client.pipelining())
    client.with_pipelining(client => {
      @async.with_task_group(group => {
        for t in 0..<32 {
          group.spawn_bg(() => {
            let n = t.to_uint64()
            let actions = client.notify_messages([
              { name: "m", args: [("n", UInt64(n))], },
            ])
            assert_eq(actions, [SetVar(Process, "proc.n", UInt64(n))])
          })
        }
      })
    })
    ignore(client.disconnect())
  })
}

///|
async test "agent-disconnect fails in-flight and later notifies" {
  let (agent_reader, client_writer) = @io.pipe()
  let (client_reader, agent_writer) = @io.pipe()
  let arrived : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      scripted_hello(agent_reader, agent_writer)
      // Read one NOTIFY, then give up on the connection.
      ignore(read_raw_frame(agent_reader))
      arrived.put(())
      agent_writer.write(
        @spop.encode_frame(
          @spop.Frame::disconnect(
            AgentDisconnect,
            ResourceAllocError,
            "out of memory",
          ),
        ),
      )
    })
    let client = @client.Client::hello(client_reader, client_writer)
    client.with_pipelining(client => {
      @async.with_task_group(group => {
        group.spawn_bg(() => {
          // In-flight when the AGENT-DISCONNECT arrives.
          try client.notify_messages([{ name: "m", args: [], }]) catch {
            @spop.SpopError(code~, ..) => assert_eq(code, ResourceAllocError)
            err => fail("unexpected error: \{err}")
          } noraise {
            _ => fail("expected ResourceAllocError")
          }
        })
      })
      // Sent after the AGENT-DISCONNECT landed on the reader task.
      arrived.get()
      try client.notify_messages([{ name: "m", args: [], }]) catch {
        @spop.SpopError(code~, ..) => assert_eq(code, ResourceAllocError)
        err => fail("unexpected error: \{err}")
      } noraise {
        _ => fail("expected ResourceAllocError")
      }
    })
  })
}

///|
async test "oversized notify is rejected before writing" {
  let agent = @agent.Agent::new(max_frame_size=256).on("m", fn(_) {
    [SetVar(Process, "proc.ok", Bool(true))]
  })
  with_session(agent, (reader, writer) => {
    let client = @client.Client::hello(reader, writer)
    assert_eq(client.peer_max_frame_size(), 256)
    let big = "x".repeat(300)
    try
      client.notify_messages([{ name: "m", args: [("data", Str(big))], }])
    catch {
      @spop.SpopError(code~, ..) => assert_eq(code, FrameTooBig)
      err => fail("unexpected error: \{err}")
    } noraise {
      _ => fail("expected FrameTooBig")
    }
    // Nothing was written: the connection is still usable.
    let actions = client.notify_messages([{ name: "m", args: [], }])
    assert_eq(actions, [SetVar(Process, "proc.ok", Bool(true))])
    ignore(client.disconnect())
  })
}

///|
async test "notify timeout raises and late acks are ignored" {
  let (agent_reader, client_writer) = @io.pipe()
  let (client_reader, agent_writer) = @io.pipe()
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      scripted_hello(agent_reader, agent_writer)
      let first = read_raw_frame(agent_reader)
      let second = read_raw_frame(agent_reader)
      // ACK the second NOTIFY immediately...
      agent_writer.write(
        @spop.encode_frame(
          @spop.Frame::ack(second.stream_id, second.frame_id, [
            SetVar(Process, "proc.order", Str("second")),
          ]),
        ),
      )
      // ...and the first one long after its timeout has expired.
      @async.sleep(150)
      agent_writer.write(
        @spop.encode_frame(
          @spop.Frame::ack(first.stream_id, first.frame_id, [
            SetVar(Process, "proc.order", Str("first")),
          ]),
        ),
      )
    })
    let client = @client.Client::hello(client_reader, client_writer)
    client.with_pipelining(client => {
      try client.notify_messages([{ name: "m", args: [], }], timeout=50) catch {
        @spop.SpopError(code~, ..) => assert_eq(code, Timeout)
        err => fail("unexpected error: \{err}")
      } noraise {
        _ => fail("expected Timeout")
      }
      let actions = client.notify_messages([{ name: "m", args: [], }])
      assert_eq(actions, [SetVar(Process, "proc.order", Str("second"))])
      // Give the late ACK time to arrive and be silently ignored.
      @async.sleep(200)
    })
  })
}

///|
async test "duplicate in-flight frame id is rejected" {
  let (agent_reader, client_writer) = @io.pipe()
  let (client_reader, agent_writer) = @io.pipe()
  let arrived : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
  let proceed : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      scripted_hello(agent_reader, agent_writer)
      let first = read_raw_frame(agent_reader)
      arrived.put(())
      // Only ACK once the duplicate-id check has played out.
      proceed.get()
      agent_writer.write(
        @spop.encode_frame(
          @spop.Frame::ack(first.stream_id, first.frame_id, []),
        ),
      )
    })
    let client = @client.Client::hello(client_reader, client_writer)
    client.with_pipelining(client => {
      let (sid, fid) = client.next_ids()
      @async.with_task_group(group => {
        let task = group.spawn(() => {
          client.notify(sid, fid, [{ name: "m", args: [], }])
        })
        arrived.get()
        try client.notify(sid, fid, [{ name: "m", args: [], }]) catch {
          @spop.SpopError(code~, ..) => assert_eq(code, FrameIdNotFound)
          err => fail("unexpected error: \{err}")
        } noraise {
          _ => fail("expected FrameIdNotFound")
        }
        proceed.put(())
        assert_eq(task.wait(), [])
      })
    })
  })
}

///|
async test "disconnect in pipelined mode returns the agent reply" {
  let agent = @agent.Agent::new().on("m", fn(_) {
    [SetVar(Process, "proc.ok", Bool(true))]
  })
  with_session(agent, (reader, writer) => {
    let client = @client.Client::hello(reader, writer)
    client.with_pipelining(client => {
      let actions = client.notify_messages([{ name: "m", args: [], }])
      assert_eq(actions, [SetVar(Process, "proc.ok", Bool(true))])
      let (code, _) = client.disconnect()
      assert_eq(code, Normal)
    })
  })
}

///|
async test "notify timeout without pipelining releases the in-flight gate" {
  // Regression test: cancellation is not catchable, so cleanup must run
  // via `defer`; a leaked gate would make every later notify time out.
  let (agent_reader, client_writer) = @io.pipe()
  let (client_reader, agent_writer) = @io.pipe()
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      scripted_hello(agent_reader, agent_writer)
      // Never ACK the first NOTIFY; ACK the second one.
      ignore(read_raw_frame(agent_reader))
      let second = read_raw_frame(agent_reader)
      agent_writer.write(
        @spop.encode_frame(
          @spop.Frame::ack(second.stream_id, second.frame_id, [
            SetVar(Process, "proc.order", Str("second")),
          ]),
        ),
      )
    })
    // No pipelining negotiated: in-flight frames are serialized by a gate.
    let client = @client.Client::hello(client_reader, client_writer, capabilities=[])
    assert_false(client.pipelining())
    client.with_pipelining(client => {
      try client.notify_messages([{ name: "m", args: [], }], timeout=50) catch {
        @spop.SpopError(code~, ..) => assert_eq(code, Timeout)
        err => fail("unexpected error: \{err}")
      } noraise {
        _ => fail("expected Timeout")
      }
      // Would hang on the leaked gate and time out before the fix.
      let actions = client.notify_messages(
        [{ name: "m", args: [], }],
        timeout=1000,
      )
      assert_eq(actions, [SetVar(Process, "proc.order", Str("second"))])
    })
  })
}

///|
async test "with_pipelining is re-enterable after cancellation" {
  // Regression test: a cancelled scope must reset the client's mode state
  // via `defer`; otherwise re-entry raises "already in pipelining mode".
  let (agent_reader, client_writer) = @io.pipe()
  let (client_reader, agent_writer) = @io.pipe()
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      scripted_hello(agent_reader, agent_writer)
      // Never ACK the first NOTIFY; ACK the second one.
      ignore(read_raw_frame(agent_reader))
      let second = read_raw_frame(agent_reader)
      agent_writer.write(
        @spop.encode_frame(
          @spop.Frame::ack(second.stream_id, second.frame_id, [
            SetVar(Process, "proc.ok", Bool(true)),
          ]),
        ),
      )
    })
    let client = @client.Client::hello(client_reader, client_writer)
    // Cancel the whole pipelined scope while a notify is in flight.
    try
      @async.with_timeout(50, () => {
        client.with_pipelining(client => {
          ignore(client.notify_messages([{ name: "m", args: [], }]))
        })
      })
    catch {
      @async.TimeoutError => ()
      err => fail("unexpected error: \{err}")
    } noraise {
      _ => fail("expected TimeoutError")
    }
    // Re-entering must work, and the connection is still usable.
    client.with_pipelining(client => {
      let actions = client.notify_messages([{ name: "m", args: [], }])
      assert_eq(actions, [SetVar(Process, "proc.ok", Bool(true))])
    })
  })
}

///|
async test "sequential mode discards late acks of timed-out notifies" {
  // Regression test: after a sequential-mode timeout the late ACK of the
  // first NOTIFY must not be mistaken for the second one's reply.
  let (agent_reader, client_writer) = @io.pipe()
  let (client_reader, agent_writer) = @io.pipe()
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      scripted_hello(agent_reader, agent_writer)
      let first = read_raw_frame(agent_reader)
      // Wait for the first NOTIFY to time out client-side, then read the
      // second one (the pipe is rendezvous-style: our late ACK below would
      // deadlock against the client's own write if we wrote it first).
      @async.sleep(150)
      let second = read_raw_frame(agent_reader)
      // The late ACK for the timed-out NOTIFY arrives first...
      agent_writer.write(
        @spop.encode_frame(
          @spop.Frame::ack(first.stream_id, first.frame_id, [
            SetVar(Process, "proc.order", Str("first")),
          ]),
        ),
      )
      // ...followed by the ACK the client is actually waiting for.
      agent_writer.write(
        @spop.encode_frame(
          @spop.Frame::ack(second.stream_id, second.frame_id, [
            SetVar(Process, "proc.order", Str("second")),
          ]),
        ),
      )
    })
    let client = @client.Client::hello(client_reader, client_writer)
    try client.notify_messages([{ name: "m", args: [], }], timeout=50) catch {
      @spop.SpopError(code~, ..) => assert_eq(code, Timeout)
      err => fail("unexpected error: \{err}")
    } noraise {
      _ => fail("expected Timeout")
    }
    // Used to raise FrameIdNotFound, mistaking the late ACK for ours.
    let actions = client.notify_messages([{ name: "m", args: [], }])
    assert_eq(actions, [SetVar(Process, "proc.order", Str("second"))])
  })
}
