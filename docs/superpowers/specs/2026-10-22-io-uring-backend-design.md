# io_uring backend for the tunnel connection

Date: 2026-10-22. Status: implemented on branch `io-uring`.

## Goal

An opt-in io_uring I/O backend for the client↔server tunnel connection, for
learning and comparison, like `-mux` is for multiplexers. The default
(Go's netpoller) stays unchanged.

Success means:

- every existing proxy test passes on the io_uring backend;
- the new direct `uringConn` tests pass;
- `go vet` and `go test -race` are clean;
- no new dependencies (standard library only);
- the benchmark reports the real difference, faster or not.

Being faster than the netpoller is **not** a goal. A one-op-per-syscall
design is not expected to win in Go; the README reports what is measured.

## Scope

In scope: Read, Write, deadlines and Close of the tunnel TCP connection on
both sides (client dial, server accept).

Out of scope: local SOCKS connections, target connections, accept and
connect through io_uring, batching, SQPOLL, registered buffers, a shared
ring reactor. Platforms other than linux/amd64 and linux/arm64.

## Where it plugs in

- New flag `-io std|uring` (default `std`) in `main.go`, validated like
  `-mux`. The wire format is identical, so each side may choose on its own;
  use the same on both sides for a fair comparison.
- New field `Config.Uring bool`. The session wraps the tunnel connection in
  exactly two places, right after it exists and before the handshake:
  - client: `Session.connect`, after `net.DialTimeout`;
  - server: `Server.handshake`, on each accepted connection.
  A wrap failure is treated like a failed connection: the client closes it
  and retries with backoff; the server closes it.
- `uringConn` implements `net.Conn`, so `Session`, `readLoop`, `writeLoop`
  and the handshake code are unchanged.
- With `-io uring`, `main` creates and destroys one probe ring before
  listening and exits with a clear error if io_uring is unavailable (kernel
  older than 5.6, `kernel.io_uring_disabled`, seccomp in containers).

### Files

| File | Content |
|---|---|
| `uring_linux.go` | ring setup and ops, `uringConn`, probe. Build tag `linux && (amd64 \|\| arm64)` because syscall numbers are hardcoded (425 setup, 426 enter). |
| `uring_other.go` | stubs for every other platform: wrapping and probing return "io_uring is not supported on this platform". |
| `uring_linux_test.go` | direct tests of `uringConn`. |

## How `uringConn` works

### Owning the socket

From the `*net.TCPConn`: duplicate the fd inside `SyscallConn().Control`,
close the Go conn (the netpoller releases its fd), and set the duplicate to
blocking mode. From then on only io_uring touches the socket. Keep
`LocalAddr`/`RemoteAddr` from the original conn.

### Rings

Two rings per connection, one for reads and one for writes, 4 entries each,
each guarded by its own mutex. Session has exactly one reader goroutine and
one writer goroutine, so they never share a queue: no reactor goroutine, no
channel hand-off.

Setup: `io_uring_setup(4, &params)`; require `IORING_FEAT_SINGLE_MMAP`; mmap
the SQ/CQ ring at offset 0 and the SQE array at `IORING_OFF_SQES`
(0x10000000) with `syscall.Mmap`. Pointers into the mmaped memory are taken
as `&mem[off]` (no pointer arithmetic), and head/tail are accessed with
`sync/atomic`.

### One operation

1. Fill an SQE: `IORING_OP_RECV` (27) for Read, `IORING_OP_SEND` (26) with
   `MSG_NOSIGNAL` for Write; put its index in the SQ array; bump the SQ tail
   with an atomic store.
2. `io_uring_enter(fd, to_submit, min_complete, IORING_ENTER_GETEVENTS)`.
3. Read the CQE(s), bump the CQ head.

`res > 0` is the byte count; `0` on a read is `io.EOF`; negative is
`-errno`. Write loops until all of `p` is sent (sends can be partial). The
buffer `p` and the timeout `timespec` are Go memory referenced by the kernel,
so every call waits for all of its CQEs before returning.

### Deadlines

`readLoop` relies on read deadlines to detect a silently dead path.
`SetReadDeadline`/`SetWriteDeadline`/`SetDeadline` store the time. When an op
runs with a deadline:

- deadline already passed: return `os.ErrDeadlineExceeded` without a syscall;
- otherwise the op SQE gets `IOSQE_IO_LINK` (1<<2) and is followed by an
  `IORING_OP_LINK_TIMEOUT` (15) SQE with the remaining time; wait for 2 CQEs
  (told apart by `user_data`):
  - data first: op CQE has the byte count, timeout CQE has `-ECANCELED`;
  - timeout first: op CQE has `-ECANCELED`, timeout CQE has `-ETIME`; the
    call returns `os.ErrDeadlineExceeded`.

### Signals

Go's scheduler preempts threads with `SIGURG`, so `io_uring_enter` returns
`EINTR` regularly. The wait is a loop: before each call it computes
`to_submit` as SQ tail minus the kernel's SQ head, and checks the CQ for
completions already posted. It never submits twice and never waits for a CQE
that has already arrived.

### Close

Closing the fd does not cancel an in-flight io_uring op (the op holds its own
reference to the file). Close, run once via `sync.Once`:

1. mark the conn closed; `shutdown(fd, SHUT_RDWR)`, which completes a pending
   recv (0 bytes) and a pending send (`EPIPE`);
2. take each ring's mutex, which waits for its op in progress to finish;
   unmap the ring and close the ring fd;
3. close the socket fd.

Read and Write after Close, or that were woken by Close, return
`net.ErrClosed`.

Minimum kernel: 5.6 (`IORING_OP_RECV`/`SEND`).

## Testing

- `eachMux` runs each test once per multiplexer **and** backend (subtests
  `custom/std`, `custom/uring`, `smux/std`, ...). Its callback becomes
  `func(t *testing.T, mux string, uring bool)` and the five tests pass
  `Uring: uring` in their `Config`. If the probe fails, the `uring` subtests
  skip with the reason. The suite takes about twice as long (~25 s).
- `uring_linux_test.go` (direct, over a loopback TCP pair):
  1. round trip of several MiB in both directions;
  2. a Read with a near deadline returns an error matching
     `os.ErrDeadlineExceeded`;
  3. Close from another goroutine unblocks a pending Read quickly, and later
     calls return `net.ErrClosed`.
- `BenchmarkThroughput` gets the same matrix: `direct`, `custom/std`,
  `custom/uring`, `smux/std`, ...
- CI (`ci.yml`) is unchanged: GitHub's Ubuntu runners are VMs with io_uring
  available; if the `uring` subtests skip, the log says why.

## Docs

- README: `-io` row in the flags table; `uring_linux.go` row in the
  architecture table; an "io_uring backend" section (how it works, kernel
  5.6+, Docker needs `security_opt: [seccomp=unconfined]` or a custom seccomp
  profile because Docker's default profile blocks io_uring); `uring` rows in
  the benchmark table with an honest explanation.
- `main.go`: add the new file to the file list in the header comment.
- `compose.yaml`: unchanged (default stays `std`).
