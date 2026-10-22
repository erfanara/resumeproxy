# io_uring for every socket, with batching reactors

Date: 2026-10-22. Status: implemented; measured 25-30% slower than `std`
because the proxy keeps about one op in flight per reactor (README, "I/O
backends"). Supersedes
`2026-10-22-io-uring-backend-design.md` (one ring per connection, one
syscall per op, measured no faster than the netpoller).

## Goal

In `-io uring` mode, every proxy socket (listeners, accepted and dialed
connections, on both client and server) does all of its I/O through io_uring,
and operations of many sockets are **batched** into few `io_uring_enter`
calls. The goal is real throughput gains over `-io std`, which stays
unchanged.

Success means:

- every proxy test passes on `std` and `uring`;
- the new direct tests pass;
- `go vet` and `go test -race` are clean;
- no new dependencies (standard library only);
- the benchmark shows batching (`ops/enter` > 1);
- the README reports `std`, `uring` and `uring-all` throughput honestly.

Beating `std` is the goal but not promised; if it does not, the README says
why, with a profile.

## Out of scope

- DNS: host names are resolved with Go's resolver, which uses its own
  sockets. Lookups are rare and do not affect throughput.
- Socket setup (`socket`, `setsockopt`, `bind`, `listen`, `getsockname`,
  `getpeername`) uses plain syscalls: io_uring's `BIND`/`LISTEN` need kernel
  6.11, GitHub's runners have 6.8, and this is one-time setup, not I/O.
- SQPOLL, multishot receive, provided buffers, registered files, splice.
- Platforms other than linux/amd64 and linux/arm64.

## Components

### `Net`

The only way the proxy opens sockets:

```go
type Net struct{ ... }                    // nil *Net = Go's net package (-io std)
func NewUringNet(rings int) (*Net, error) // starts the reactors
func (n *Net) Listen(addr string) (net.Listener, error)
func (n *Net) Dial(addr string, timeout time.Duration) (net.Conn, error)
func (n *Net) Close()
func (n *Net) Stats() (ops, enters uint64) // for ops/enter
```

On a nil `*Net`, `Listen`/`Dial` call `net.Listen`/`net.DialTimeout`.

`Config.Uring bool` becomes `Config.Net *Net`. Call sites:

| Where | Before | After |
|---|---|---|
| `main.go` listener (SOCKS / tunnel) | `net.Listen` | `nw.Listen` |
| `Session.connect` | `net.DialTimeout` + `tunnelConn` | `cfg.Net.Dial(addr, cfg.Timeout)` |
| `Server.handshake` | `tunnelConn` wrap | nothing (conn is already io_uring) |
| `dialTarget` (`mux.go`, `libmux.go`) | `net.DialTimeout` | `Net.Dial(addr, 10s)`, passed in |

`socks.go`, `Mux`, `libmux.go` (`join`/`pipe`) and `Session` keep seeing only
`net.Conn`/`net.Listener`. The io_uring conn implements `CloseWrite`, used
for half-close by `Mux` and `pipe`.

New flag `-uring-rings N` (only with `-io uring`); its default is chosen by
the benchmark sweep. `main` calls `NewUringNet` once, which also probes the
kernel and exits with a clear error if io_uring is unusable.

### Files

| File | Content |
|---|---|
| `uring_linux.go` | the ring: setup with flags, mmap, SQE/CQE layout (today's code, adapted) |
| `uringnet_linux.go` | reactor, `Net`, listener, dial, conn |
| `uring_other.go` | stubs: `NewUringNet` returns "not supported on this platform" |
| `uringnet_linux_test.go` | direct tests (replaces `uring_linux_test.go`) |

Today's per-connection `uringConn` is removed. Build tag for the Linux files:
`linux && (amd64 || arm64)`. Minimum kernel: 6.1.

## Reactor

`NewUringNet(rings)` starts `rings` reactors. Each reactor is one ring and one
goroutine with `runtime.LockOSThread`. Every new socket (listener, accepted
or dialed conn) is pinned to one reactor, round-robin, for its whole life.

Ring: 256 SQ entries (512 CQ), flags `SINGLE_ISSUER | DEFER_TASKRUN |
SUBMIT_ALL`. Only the reactor's locked thread calls `io_uring_enter`, so the
kernel runs completion work only when that thread enters.

### Submitting an op (any goroutine)

1. Fill a request: opcode, fd, buffer, extra args, deadline, and the conn
   direction's `done` channel (one per conn direction, reused: no allocation
   per op).
2. Lock the reactor queue, append, read and clear the `asleep` flag, unlock.
   If it was asleep, write 8 bytes to the reactor's eventfd (the only
   syscall a submitter makes).
3. Wait on `done`. The buffer stays referenced by the waiting goroutine; Go's
   heap does not move, so the kernel's pointer stays valid.

### Reactor loop

```
loop:
  lock queue; take queued requests (up to free slots); if none: asleep = true; unlock
  for each request: SQE (+ LINK_TIMEOUT SQE if it has a deadline)
  io_uring_enter(to_submit, min_complete = 1, GETEVENTS)   // the batch
  asleep = false (under lock)
  for each CQE: find request by user_data, deliver result on done
```

- One `READ` of the eventfd into an 8-byte buffer is always in flight and is
  re-armed after it completes, so `min_complete = 1` returns on a socket
  completion or a wake-up.
- `asleep` is set under the queue lock in the same critical section that saw
  the queue empty; submitters read it under the same lock. A request is
  therefore either picked up next round or triggers the eventfd write.
- `user_data` is a slot index into the reactor's in-flight table (never a Go
  pointer); the top bit marks the linked timeout's CQE.
- At most 256 requests in flight (each makes at most 2 CQEs, so the CQ cannot
  overflow); the rest wait in the queue.
- `EINTR` (Go preempts threads with `SIGURG`): `to_submit` is recomputed from
  the kernel's SQ head on every call; nothing is submitted twice.
- Counters: socket ops completed and `io_uring_enter` calls, for `Stats`.

### Ops

| API | io_uring op | Result mapping |
|---|---|---|
| `Read` | `RECV` | `0` → `io.EOF` |
| `Write` | `SEND` + `MSG_NOSIGNAL` | loop until all of `p` is sent |
| `CloseWrite` | `SHUTDOWN(SHUT_WR)` | |
| `Close` | `ASYNC_CANCEL` (by fd, all) then `CLOSE` | pending ops wake with `-ECANCELED` → `net.ErrClosed` |
| `Accept` | `ACCEPT` + `SOCK_CLOEXEC` | new fd |
| `Dial` | `CONNECT` | `0` = connected |

Deadlines: `LINK_TIMEOUT` linked behind the op; an already passed deadline
returns `os.ErrDeadlineExceeded` without submitting. Timeout → op CQE
negative, timeout CQE `-ETIME` → `os.ErrDeadlineExceeded`.

Errors: `io.EOF`, `net.ErrClosed`, `os.ErrDeadlineExceeded` (a
`net.Error` with `Timeout()`), otherwise `os.NewSyscallError(op, errno)` with
`op` one of `recv`, `send`, `connect`, `accept`, `shutdown`.

## Listen, accept, dial

- **Listen(addr):** `net.ResolveTCPAddr`; `socket(SOCK_STREAM|SOCK_CLOEXEC)`,
  `SO_REUSEADDR`, `bind`, `listen(4096)`. Empty host: IPv6 dual stack
  (`IPV6_V6ONLY=0`), IPv4 if IPv6 is unavailable. `Addr()` from
  `getsockname`. `Close()` cancels the pending `ACCEPT` and closes the fd;
  `Accept` then returns `net.ErrClosed`.
- **Accept():** `ACCEPT` on the listener's reactor; the new socket gets
  `TCP_NODELAY` (Go sets it on every TCP conn; the comparison must be fair)
  and the next reactor round-robin; addresses from
  `getsockname`/`getpeername`.
- **Dial(addr, timeout):** split host/port; IP literal or
  `net.DefaultResolver.LookupIPAddr` within the timeout; try addresses in
  order until one connects, within the timeout. Per address: socket with
  `SOCK_CLOEXEC` (blocking mode), `TCP_NODELAY`, `CONNECT` with a linked
  timeout for the remaining time. On failure the fd is closed.
- Sockets stay in blocking mode: an op that must wait parks in the kernel
  (io_uring's internal poll) and never returns `EAGAIN` to us.

## Testing

`setup(t, cfg, mux, io)`:

| `io` | proxy client | proxy server | test harness (relay, echo, SOCKS dialer) |
|---|---|---|---|
| `std` | Go net | Go net | Go net |
| `uring` | own `Net` | own `Net` | Go net |
| `uring-all` | own `Net` | own `Net` | own `Net` |

Separate `Net`s mimic separate processes, so batching numbers are not
inflated by sharing a ring.

- `eachMux` runs every proxy test on `std` and `uring`; `uring-all` runs in
  the benchmark only. `uring` subtests skip with the reason when the probe
  fails (including kernels older than 6.1).
- `uringnet_linux_test.go`:
  1. 8 MiB echoed both ways at once over `Listen`/`Dial`/`Accept`;
  2. a read deadline returns `os.ErrDeadlineExceeded`, the conn still works;
  3. `Close` unblocks a pending `Read`; later calls return `net.ErrClosed`;
  4. listener `Close` unblocks a pending `Accept`;
  5. `CloseWrite`: the peer sees EOF and can still send back;
  6. dialing a closed port fails quickly with connection refused;
  7. batching: 32 connections echo concurrently on one reactor and `Stats`
     shows more than 1 op per enter.
- `BenchmarkThroughput`: `direct`, then `custom|smux|yamux` ×
  `std|uring|uring-all`; `uring*` variants report `ops/enter` (proxy reactors
  only) with `b.ReportMetric`. Test flag `-uring-rings=N` sets the reactor
  count; a sweep over 1, 2, 4, 8 picks the default.
- CI unchanged (GitHub runners: kernel 6.8).

## Docs

- README "I/O backends": rewritten for the reactor design; keep the SQPOLL
  lesson; new measured numbers including the rings sweep.
- Flags table: `-uring-rings`. Architecture table: the new files.
  Limitations: kernel 6.1+, DNS through Go's resolver, Docker seccomp.
- `main.go` header comment: file list.
