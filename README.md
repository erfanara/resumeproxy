# ResumeProxy: Transparent TCP Session Recovery

An educational proof of concept: a SOCKS5 proxy whose client↔server tunnel
**survives TCP connection loss without closing the local connections**.

If the tunnel TCP connection is killed, reset, or silently blackholed, the
apps using the proxy see only a pause, never a broken connection. Once a new
tunnel connection is up, every byte continues exactly where it stopped: none
are lost, duplicated, or reordered.

![Animation: the same download through a normal proxy tunnel and through resumeproxy. The tunnel TCP connection dies in both. The normal proxy resets the app's connection and the download is lost; with resumeproxy the app only sees a pause, the tunnel is redialed, and the download finishes with nothing lost or duplicated.](docs/tunnel-loss.svg)

> PoC only: the tunnel has no encryption or authentication. Run it inside a
> real transport (TLS, etc.).

## Architecture

The code has three layers. Each layer solves one problem and knows nothing
about the problems of the others.

```mermaid
flowchart LR
    App[App] -- SOCKS5 --> C
    subgraph Client side
        C[SOCKS5 + Client<br/>socks.go, main.go] --> MC[Mux<br/>mux.go or libmux.go] --> SC[Session<br/>session.go]
    end
    SC == "tunnel TCP<br/>(may die and be redialed)" ==> SS
    subgraph Server side
        SS[Session<br/>session.go] --> MS[Mux<br/>mux.go or libmux.go] --> D[Dialer<br/>main.go]
    end
    D -- TCP --> T[Target]
```

| Layer   | File         | Responsibility                                                                 |
|---------|--------------|--------------------------------------------------------------------------------|
| Session | `session.go` | One reliable byte stream that outlives the TCP connection carrying it          |
| Mux     | `mux.go`     | Many proxied connections ("streams") multiplexed over one Session              |
|         | `libmux.go`  | The same job done by smux or yamux, selected with `-mux`                       |
| Front   | `socks.go`   | SOCKS5 server on the client (parses untrusted input)                           |
|         | `main.go`    | Client session holder, target dialer on the server, CLI flags                  |

The key design choice: **resumption is solved once, for the whole byte
stream**, in the Session. The Mux never sees a reconnect; to it, the Session
simply never fails unless the grace period runs out. That is also why an
off-the-shelf multiplexer can be swapped in (see
[Multiplexers](#multiplexers-custom-smux-yamux)).

## Quick start

```sh
go build -o resumeproxy .

# on the server
./resumeproxy -mode server -listen :9000

# on the client
./resumeproxy -mode client -listen 127.0.0.1:1080 -server HOST:9000

# use it
curl --socks5-hostname 127.0.0.1:1080 https://example.com
```

### Flags

| Flag         | Default          | Meaning                                                      |
|--------------|------------------|--------------------------------------------------------------|
| `-mode`      | (required)       | `client` or `server`                                         |
| `-listen`    | `127.0.0.1:1080` | SOCKS5 address (client) or tunnel address (server)           |
| `-server`    | (client only)    | Address of the proxy server                                  |
| `-mux`       | `custom`         | `custom`, `smux` or `yamux`; must be the same on both sides  |
| `-grace`     | `60s`            | How long a session is kept while the tunnel is down          |
| `-heartbeat` | `5s`             | Ping interval; the tunnel counts as dead after 3 missed      |
| `-buf`       | `4`              | Retransmit buffer per direction, in MiB                      |

## Try it yourself

Watch a download survive a dead network, all on one machine. `socat` plays
the network between the proxy client and the proxy server, so killing it cuts
the tunnel.

```sh
go build -o resumeproxy .

# a target: a local web server with a 60 MiB file
mkdir -p /tmp/demo && head -c 60M /dev/urandom > /tmp/demo/big.bin
python3 -m http.server 8000 -d /tmp/demo &

# the proxy server, the "network" in front of it, and the proxy client
./resumeproxy -mode server -listen 127.0.0.1:9000 -heartbeat 1s &
socat TCP-LISTEN:9001,fork,reuseaddr TCP:127.0.0.1:9000 &
./resumeproxy -mode client -listen 127.0.0.1:1080 -server 127.0.0.1:9001 -heartbeat 1s &

# a slow download through the proxy
curl --socks5-hostname 127.0.0.1:1080 --limit-rate 5M -o /tmp/demo/copy.bin http://localhost:8000/big.bin
```

While curl runs, break the network from a second terminal:

```sh
pkill socat          # cut it: curl pauses, nothing breaks
socat TCP-LISTEN:9001,fork,reuseaddr TCP:127.0.0.1:9000 &   # restore it: curl continues
pkill -STOP socat    # blackhole: packets vanish, no FIN or RST is ever sent
pkill -CONT socat    # once the logs say "tunnel lost" (about 3 s), let traffic through again
```

Then check that nothing was lost or duplicated:

```sh
cmp /tmp/demo/big.bin /tmp/demo/copy.bin && echo identical
```

The logs show each step: when the tunnel was lost, how many bytes were still
unacknowledged, and how many were replayed after the reconnect. curl speeds up
after each pause because `--limit-rate` limits the *average* rate. On Linux
with root you can also kill the tunnel socket directly:
`sudo ss -K dport = :9000`.

## How resumption works

TCP only guarantees delivery while the connection is alive. When it dies,
bytes our kernel accepted may never have reached the peer application, and
there is no way to tell how many. So each side counts bytes at the
application layer:

- **Sender** keeps every byte in a retransmit buffer until the peer ACKs it.
- **Receiver** remembers `recvOff`, the total number of bytes received in order.
- **On reconnect** both sides exchange their `recvOff`, and each side resends
  everything after the peer's offset.

### Send-side offsets

```
        acked by peer      sent, unacked     not yet sent
  ... ───────────────┼──────────────────────┼──────────────┤
                  sendBase               sendNext    sendBase+len(sendBuf)
                     └─────────────── sendBuf ─────────────┘
```

On resume, `sendNext` is rewound to the peer's `recvOff`.

### Reconnect sequence

```mermaid
sequenceDiagram
    participant C as Client Session
    participant S as Server Session
    C->>S: DATA (bytes 0..999)
    S-->>C: ACK 600
    Note over C,S: tunnel TCP dies, bytes 600..999 are in limbo
    Note over C: local sockets just block (backpressure)
    C->>S: new TCP: hello "RSP1" | id | token | recvOff
    Note over S: detach old tunnel so recvOff stops moving
    S-->>C: reply ok | recvOff = 800
    C->>S: DATA resent from 800
    Note over C,S: no gap, no duplicate
```

### Session lifecycle

```mermaid
stateDiagram-v2
    [*] --> Detached: created
    Detached --> Attached: handshake ok
    Attached --> Detached: TCP error / heartbeat timeout
    Detached --> Dead: grace period expired
    Attached --> Dead: protocol error
    Dead --> [*]: all streams closed
```

When a session dies, the Mux closes every local connection, and the Client
transparently creates a fresh session for the next SOCKS request.

### Failure detection

| Failure                         | How it is detected                                         |
|---------------------------------|------------------------------------------------------------|
| Connection closed / reset       | Read or write error on the tunnel                          |
| Silent blackhole (no FIN/RST)   | No frame (not even a PING) within `Timeout` = 3 × heartbeat |
| Reconnect fails                 | Client redials with backoff: 50 ms, doubling, max 2 s       |
| Outage longer than grace period | Session is closed, local connections see EOF                |
| Server forgot the session       | Handshake is rejected and the client gives up on it         |

## Multiplexers: custom, smux, yamux

The Mux layer can be swapped with `-mux` (use the same value on both sides).
All three run over the same resumable Session, so none of them ever sees a
reconnect.

|                            | `custom` (`mux.go`)                        | `smux`                                      | `yamux`                                         |
|----------------------------|--------------------------------------------|---------------------------------------------|-------------------------------------------------|
| Code                       | about 190 lines in this repo               | [xtaci/smux](https://github.com/xtaci/smux), used by kcptun | [hashicorp/yamux](https://github.com/hashicorp/yamux), used by Consul and Nomad |
| Per-stream flow control    | No: one app that stops reading stalls every stream | Yes (protocol v2, 64 KiB window per stream) | Yes (256 KiB window per stream)                 |
| Half-close                 | `mFin` frame                               | `CloseWrite`                                | `Close` (in yamux it is a half-close)           |
| "Server reached the target" | `mOpenOK` / `mRst` frames                 | 1-byte status header from `libmux.go`       | same as smux                                    |
| Changed for resumption     | nothing                                    | keepalive off                               | keepalive off, write timeout raised, open timeout off |

**Lesson: libraries assume a live connection.** Both libraries ship keepalives
and timeouts that close the whole mux when the connection below them stalls.
Over a Session that can pause for a full grace period, every outage would
become a broken connection, which is exactly what this project prevents. So
`libmux.go` turns them off and lets the Session decide when the tunnel is
dead. One smux timeout cannot be changed: opening or closing a stream gives up
after 30 s if the Session's retransmit buffer stays full that long.

`TestSlowStreamDoesNotStallOthers` shows the flow-control difference: with
smux or yamux a second stream keeps working while the first one's app has
stopped reading. With `mux.go` the whole session stalls, so that subtest is
skipped.

## Wire format

### Session layer (`session.go`)

| Message | Direction       | Layout                                              |
|---------|-----------------|-----------------------------------------------------|
| hello   | client → server | `"RSP1"` · id[16] · token[16] · recvOff u64         |
| reply   | server → client | status u8 (0 = ok) · recvOff u64                    |
| DATA    | both            | `0` · len u16 · payload (≤ 16 KiB)                  |
| ACK     | both            | `1` · recvOff u64                                   |
| PING    | both            | `2`                                                 |

The `token` is a secret that proves a reconnecting client owns the session;
the server compares it in constant time.

### Mux layer (`mux.go`)

Each frame is `streamID u32 · type u8 · len u16 · payload`.

| Type     | Meaning                                  |
|----------|------------------------------------------|
| `mOpen`  | Open a stream, payload is `host:port`    |
| `mOpenOK`| Server connected to the target           |
| `mData`  | Stream data                              |
| `mFin`   | Sender will write no more (half-close)   |
| `mRst`   | Stream is gone                           |

```mermaid
sequenceDiagram
    participant A as App
    participant C as Client Mux
    participant S as Server Mux
    participant T as Target
    A->>C: SOCKS5 CONNECT host:port
    C->>S: mOpen "host:port"
    S->>T: dial
    alt connected
        S-->>C: mOpenOK
        C-->>A: SOCKS reply 0 (success)
        A->>T: mData both ways
        A->>C: EOF → mFin
        T->>S: EOF → mFin
        Note over C,S: stream freed once both sides finished
    else dial failed
        S-->>C: mRst
        C-->>A: SOCKS reply 5 (refused)
    end
```

The SOCKS success reply is delayed until the server has really connected, so
the app gets a real "connection refused" instead of a fake success.

### Library streams (`libmux.go`)

smux and yamux frame the streams themselves (see their repos). They only
carry bytes, so each stream starts with a small header before the proxied
data:

| Message | Direction       | Layout                          |
|---------|-----------------|---------------------------------|
| target  | client → server | len u16 · `host:port`           |
| status  | server → client | u8 (0 = connected, else failed) |

## Tests

```sh
go test ./...
```

The tests put a misbehaving relay between client and server and push data
through a real SOCKS5 connection to an echo server. Each test runs once per
multiplexer as a subtest, for example `go test -run 'Kills/yamux' -v`.

| Test                                     | What it proves                                                        |
|------------------------------------------|-----------------------------------------------------------------------|
| `TestSurvivesRepeatedTunnelKills`        | 64 MiB echoed intact while the tunnel is killed every 15 ms           |
| `TestSurvivesSilentBlackhole`            | Heartbeat detects a tunnel that drops packets without FIN/RST         |
| `TestMultipleStreamsSurvive`             | 8 parallel streams survive repeated kills                             |
| `TestGraceExpiryClosesLocalThenRecovers` | Local connection closes only after grace, and new ones work afterward |
| `TestSlowStreamDoesNotStallOthers`       | A stream whose app stops reading doesn't block others (smux, yamux)   |

### Benchmark

```sh
go test -run '^$' -bench Throughput
```

Echoes 8 MiB per iteration over a direct TCP connection and through the whole
proxy with each multiplexer. One run on a laptop (i5-1235U):

| Path     | Throughput  |
|----------|-------------|
| direct   | ~1950 MB/s  |
| `custom` | ~260 MB/s   |
| `smux`   | ~285 MB/s   |
| `yamux`  | ~310 MB/s   |

The proxied path adds three TCP hops (proxy client, test relay, proxy server)
and several buffer copies. That costs far more than the choice of
multiplexer. Numbers vary from run to run.

## Related work

The same problems, solved elsewhere:

- **[Eternal Terminal](https://eternalterminal.dev/)**: a reconnecting remote
  shell over TCP. Like this project, it buffers sent bytes and replays them
  after a reconnect.
- **[Mosh](https://mosh.org/)**: keeps a terminal alive across roaming and
  sleep, but syncs screen *state* over UDP instead of resuming a byte stream.
- **QUIC connection migration** ([RFC 9000 §9](https://www.rfc-editor.org/rfc/rfc9000#section-9)):
  connections are identified by connection IDs, not by addresses, so the
  transport itself survives a change of network path.
- **Multipath TCP** ([RFC 8684](https://www.rfc-editor.org/rfc/rfc8684)): one
  connection spread over several TCP subflows; losing one subflow does not
  break it.
- **autossh**: the opposite approach. It restarts the SSH connection, so every
  connection forwarded through it breaks.
- **HTTP/2 and QUIC streams**: multiplexing many streams over one connection
  with per-stream flow control, like smux and yamux.

## Known limitations

| Limitation                       | Effect                                                                 |
|----------------------------------|------------------------------------------------------------------------|
| No encryption / authentication   | Must run inside a secure transport. Without client authentication the server is an open proxy: anyone who reaches it can use it, including to reach the server's own localhost and LAN |
| No per-stream flow control in `mux.go` | One local socket that stops reading stalls other streams of the session; `-mux smux` and `-mux yamux` don't have this problem |
| `-mux` mismatch                  | Not detected: if client and server use different multiplexers, streams just fail |
| Server restart                   | All sessions are lost; clients start new ones                          |
| SOCKS5 subset                    | No auth, `CONNECT` only                                                |

## License

[GPL-3.0](LICENSE).

---

Written by [Claude](https://claude.com/claude-code).
