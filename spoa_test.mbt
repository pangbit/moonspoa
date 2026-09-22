///|
/// The root package re-exports the protocol core and the agent types, so
/// they are all reachable through `@moonspoa`.
test "re-exported types are usable from the root package" {
  let message : @moonspoa.Message = {
    name: "check-ip",
    args: [("ip", Str("1.2.3.4")), ("score", Int32(85))],
  }
  assert_eq(message.name, "check-ip")
  assert_eq(message.args.length(), 2)
  let frame = @moonspoa.Frame::notify(0, 1, [message])
  assert_eq(frame.frame_type, Notify)
  let action : @moonspoa.Action = SetVar(Transaction, "txn.score", Int32(85))
  ignore(action)
  let agent = @moonspoa.Agent::new().on("check-ip", fn(msg) {
    [SetVar(Transaction, "txn.seen", Str(msg.name))]
  })
  assert_true(agent.handlers.contains("check-ip"))
  let session = @moonspoa.Session::new(agent)
  assert_false(session.pipelining())
}
