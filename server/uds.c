// C stub for Unix domain socket transport of the SPOA server.
//
// All sockaddr_un handling lives here (not in MoonBit) because the layout
// of struct sockaddr_un differs between macOS (sun_len field) and Linux.
//
// Error convention: functions return a non-negative file descriptor (or 0
// for spoa_uds_unlink) on success, and -errno on failure.

#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include "moonbit.h"

// Set O_NONBLOCK and FD_CLOEXEC on `fd`. Returns 0 on success, -1 with
// errno set on failure.
static int spoa_uds_set_flags(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    return -1;
  }
  flags = fcntl(fd, F_GETFD, 0);
  if (flags < 0 || fcntl(fd, F_SETFD, flags | FD_CLOEXEC) < 0) {
    return -1;
  }
  return 0;
}

// Fill `addr` for `path` (a null-terminated string) and compute the
// address length. Returns 0 on success, -ENAMETOOLONG if the path does not
// fit into sun_path.
static int spoa_uds_make_addr(
  struct sockaddr_un *addr,
  const char *path,
  socklen_t *addrlen
) {
  size_t path_len = strlen(path);
  if (path_len >= sizeof(addr->sun_path)) {
    return -ENAMETOOLONG;
  }
  memset(addr, 0, sizeof(*addr));
  addr->sun_family = AF_UNIX;
  memcpy(addr->sun_path, path, path_len);
#ifdef SUN_LEN
  *addrlen = SUN_LEN(addr);
#else
  *addrlen =
    (socklen_t)(offsetof(struct sockaddr_un, sun_path) + path_len + 1);
#endif
  return 0;
}

MOONBIT_FFI_EXPORT
int32_t spoa_uds_listen(moonbit_bytes_t path, int32_t backlog) {
  int saved;
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    return -errno;
  }
  if (spoa_uds_set_flags(fd) < 0) {
    saved = errno;
    close(fd);
    return -saved;
  }
  struct sockaddr_un addr;
  socklen_t addrlen;
  int ret = spoa_uds_make_addr(&addr, (const char *)path, &addrlen);
  if (ret < 0) {
    close(fd);
    return ret;
  }
  // If the path is occupied, only a stale socket file left over by a
  // previous run may be removed. A regular file (or anything else) is
  // reported occupied without being touched, and so is a socket that
  // still has a live listener: staleness is confirmed by a failed
  // connect() before unlinking.
  struct stat st;
  if (lstat((const char *)path, &st) == 0) {
    if (!S_ISSOCK(st.st_mode)) {
      close(fd);
      return -EADDRINUSE;
    }
    int probe = socket(AF_UNIX, SOCK_STREAM, 0);
    if (probe < 0) {
      saved = errno;
      close(fd);
      return -saved;
    }
    int probe_ret = connect(probe, (struct sockaddr *)&addr, addrlen);
    saved = errno;
    close(probe);
    if (probe_ret == 0) {
      // A live listener answered; the path is still in service.
      close(fd);
      return -EADDRINUSE;
    }
    if (saved != ECONNREFUSED && saved != ENOENT) {
      close(fd);
      return -saved;
    }
    if (unlink((const char *)path) < 0) {
      saved = errno;
      close(fd);
      return -saved;
    }
  } else if (errno != ENOENT) {
    saved = errno;
    close(fd);
    return -saved;
  }
  if (bind(fd, (struct sockaddr *)&addr, addrlen) < 0) {
    saved = errno;
    close(fd);
    return -saved;
  }
  if (listen(fd, backlog) < 0) {
    saved = errno;
    close(fd);
    return -saved;
  }
  return fd;
}

// Accept one pending connection from `listen_fd` (which must be
// non-blocking). Returns the connection fd (> 0) with O_NONBLOCK and
// FD_CLOEXEC set, 0 if no connection is pending (EAGAIN), and -errno on
// failure. The peer address is not needed and therefore not returned.
MOONBIT_FFI_EXPORT
int32_t spoa_uds_accept(int32_t listen_fd) {
  int fd = accept(listen_fd, NULL, NULL);
  if (fd < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return 0;
    }
    return -errno;
  }
  if (fd == 0) {
    // fd 0 is reserved as the EAGAIN sentinel; it can only be returned by
    // accept if stdin was closed, so move it out of the way.
    int moved = fcntl(fd, F_DUPFD_CLOEXEC, 1);
    if (moved >= 0) {
      close(fd);
      fd = moved;
    }
  }
  if (spoa_uds_set_flags(fd) < 0) {
    int saved = errno;
    close(fd);
    return -saved;
  }
  return fd;
}

MOONBIT_FFI_EXPORT
int32_t spoa_uds_connect(moonbit_bytes_t path) {  int saved;
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    return -errno;
  }
  if (spoa_uds_set_flags(fd) < 0) {
    saved = errno;
    close(fd);
    return -saved;
  }
  struct sockaddr_un addr;
  socklen_t addrlen;
  int ret = spoa_uds_make_addr(&addr, (const char *)path, &addrlen);
  if (ret < 0) {
    close(fd);
    return ret;
  }
  if (connect(fd, (struct sockaddr *)&addr, addrlen) < 0 && errno != EINPROGRESS) {
    saved = errno;
    close(fd);
    return -saved;
  }
  return fd;
}

MOONBIT_FFI_EXPORT
int32_t spoa_uds_unlink(moonbit_bytes_t path) {
  if (unlink((const char *)path) < 0) {
    return -errno;
  }
  return 0;
}
