///|
/// FFI declarations for the Unix domain socket C stub (`uds.c`).
/// sockaddr handling lives entirely in C because the layout of
/// `struct sockaddr_un` differs between macOS and Linux.
///
/// Error convention: functions return a non-negative fd (or 0) on success
/// and `-errno` on failure; `uds_accept_ffi` additionally returns 0 when
/// no connection is pending (EAGAIN).

///|
#borrow(path)
extern "C" fn uds_listen_ffi(path : Bytes, backlog : Int) -> Int = "spoa_uds_listen"

///|
extern "C" fn uds_accept_ffi(listen_fd : Int) -> Int = "spoa_uds_accept"

///|
#borrow(path)
extern "C" fn uds_connect_ffi(path : Bytes) -> Int = "spoa_uds_connect"

///|
#borrow(path)
extern "C" fn uds_unlink_ffi(path : Bytes) -> Int = "spoa_uds_unlink"

///|
/// Errors of the server transport layer.
pub suberror ServerError {
  /// Failed to create/bind/listen on a Unix domain socket.
  UdsListen(path~ : String, errno~ : Int)
  /// Failed to accept on a Unix domain socket listener.
  UdsAccept(errno~ : Int)
  /// Failed to connect to a Unix domain socket.
  UdsConnect(path~ : String, errno~ : Int)
} derive(Debug)

///|
/// Connect to the Unix domain socket at `path` (client side).
pub async fn connect_unix(path : String) -> @raw_fd.RawFdStream {
  let fd = uds_connect_ffi(@utf8.encode(path))
  if fd < 0 {
    raise UdsConnect(path~, errno=-fd)
  }
  @raw_fd.RawFdStream(fd)
}
