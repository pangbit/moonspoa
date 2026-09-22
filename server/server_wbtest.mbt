///|
/// White-box tests for the UDS C stub: path-occupation policies of
/// `uds_listen_ffi` that cannot be exercised through the public API
/// (`Server::close` always removes the socket file, so a stale socket
/// can only be simulated by closing the fd behind its back).
///
/// errno values differ between platforms; assertions stay on the sign of
/// the result. (The `fs` package is not importable here: whitebox tests
/// share the package's own imports, and `fs` is a blackbox-test import.)

///|
extern "C" fn c_close(fd : Int) -> Int = "close"

///|
async test "uds_listen replaces a stale socket file" {
  let path = "/tmp/spoa-wbtest-stale-\{@async.now()}.sock"
  let fd = uds_listen_ffi(@utf8.encode(path), 128)
  assert_true(fd >= 0)
  // Simulate a crashed server: the listener fd is gone but the socket
  // file remains. A fresh bind must detect it as stale (connect refused)
  // and take over the path — a second bind would fail with EADDRINUSE
  // had the file not been removed.
  ignore(c_close(fd))
  let fd2 = uds_listen_ffi(@utf8.encode(path), 128)
  assert_true(fd2 >= 0)
  ignore(c_close(fd2))
  assert_eq(uds_unlink_ffi(@utf8.encode(path)), 0)
}

///|
async test "uds_listen refuses a path with a live listener" {
  let path = "/tmp/spoa-wbtest-live-\{@async.now()}.sock"
  let fd = uds_listen_ffi(@utf8.encode(path), 128)
  assert_true(fd >= 0)
  // The kernel accepts the probe connection on the live listener, so the
  // second bind reports the path as occupied (EADDRINUSE) instead of
  // unlinking it.
  let fd2 = uds_listen_ffi(@utf8.encode(path), 128)
  assert_true(fd2 < 0)
  ignore(c_close(fd))
  assert_eq(uds_unlink_ffi(@utf8.encode(path)), 0)
}
