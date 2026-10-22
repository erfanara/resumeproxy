package main

import (
	"bytes"
	"crypto/rand"
	"flag"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// relay sits between proxy client and proxy server and misbehaves on demand.
type relay struct {
	ln       net.Listener
	upstream string
	mu       sync.Mutex
	conns    map[*relayConn]bool
	down     bool
}

type relayConn struct {
	a, b   net.Conn
	frozen atomic.Bool
}

func newRelay(t testing.TB, nw *Net, upstream string) *relay {
	r := &relay{ln: listen(t, nw), upstream: upstream, conns: map[*relayConn]bool{}}
	go func() {
		for {
			a, err := r.ln.Accept()
			if err != nil {
				return
			}
			r.mu.Lock()
			down := r.down
			r.mu.Unlock()
			if down {
				a.Close()
				continue
			}
			b, err := nw.Dial(upstream, 10*time.Second)
			if err != nil {
				a.Close()
				continue
			}
			rc := &relayConn{a: a, b: b}
			r.mu.Lock()
			r.conns[rc] = true
			r.mu.Unlock()
			go rc.pipe(a, b)
			go rc.pipe(b, a)
		}
	}()
	return r
}

func (rc *relayConn) pipe(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 && !rc.frozen.Load() { // frozen: silently swallow, like a blackholed path
			if _, werr := dst.Write(buf[:n]); werr != nil {
				err = werr
			}
		}
		if err != nil {
			rc.a.Close()
			rc.b.Close()
			return
		}
	}
}

// Kill abruptly closes every tunnel connection; in-flight bytes are lost.
func (r *relay) Kill() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for rc := range r.conns {
		rc.a.Close()
		rc.b.Close()
		delete(r.conns, rc)
	}
}

// Freeze blackholes existing connections without closing them.
func (r *relay) Freeze() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for rc := range r.conns {
		rc.frozen.Store(true)
	}
}

func (r *relay) SetDown(down bool) {
	r.mu.Lock()
	r.down = down
	r.mu.Unlock()
	if down {
		r.Kill()
	}
}

func listen(t testing.TB, nw *Net) net.Listener {
	ln, err := nw.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return ln
}

type env struct {
	client  *Client
	relay   *relay
	harness *Net   // sockets of the relay, echo server and SOCKS dialer
	proxy   []*Net // proxy server and client sockets (nil entries for std)
	socks   string
	echo    string
}

// startEcho runs a target server that sends back everything it receives.
func startEcho(t testing.TB, nw *Net) string {
	ln := listen(t, nw)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { io.Copy(c, c); c.Close() }()
		}
	}()
	return ln.Addr().String()
}

// setup starts a proxy server and client with the relay between them. mode
// picks the sockets: "std" (Go's net), "uring" (the proxy on io_uring, the
// harness on Go's net) or "uring-all" (the harness too). Server, client and
// harness get separate Nets, like separate processes.
func setup(t testing.TB, cfg Config, mux, mode string) *env {
	cfg.Logf = testLogf(t)
	srvCfg, cliCfg := cfg, cfg
	var harness *Net
	if mode != "std" {
		srvCfg.Net, cliCfg.Net = newTestNet(t), newTestNet(t)
		if mode == "uring-all" {
			harness = newTestNet(t)
		}
	}
	srvLn := listen(t, srvCfg.Net)
	go (&Server{Cfg: srvCfg, Handler: serverHandler(mux)}).Serve(srvLn)
	r := newRelay(t, harness, srvLn.Addr().String())
	cl := &Client{Server: r.ln.Addr().String(), Mux: mux, Cfg: cliCfg}
	socksLn := listen(t, cliCfg.Net)
	go cl.ServeSocks(socksLn)
	return &env{client: cl, relay: r, harness: harness, proxy: []*Net{srvCfg.Net, cliCfg.Net},
		socks: socksLn.Addr().String(), echo: startEcho(t, harness)}
}

// testLogf passes session logs to t until t ends, then drops them: sessions
// outlive their test, and testing panics on a Log after the test completed.
func testLogf(t testing.TB) func(string, ...any) {
	var mu sync.Mutex
	done := false
	t.Cleanup(func() {
		mu.Lock()
		done = true
		mu.Unlock()
	})
	return func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		if !done {
			t.Logf(format, args...)
		}
	}
}

// dial opens a SOCKS connection to the echo target through the proxy.
func (e *env) dial(t testing.TB) net.Conn { return socksDial(t, e.harness, e.socks, e.echo) }

// attaches reports how many tunnel connections the current session has used.
func (e *env) attaches() int {
	e.client.mu.Lock()
	defer e.client.mu.Unlock()
	return e.client.sess.Attaches()
}

var muxes = []string{"custom", "smux", "yamux"}

// eachMux runs f once per multiplexer and socket mode ("std", "uring"; see
// setup); the resumable Session in between is the same.
func eachMux(t *testing.T, f func(t *testing.T, mux, mode string)) {
	for _, mux := range muxes {
		for _, mode := range []string{"std", "uring"} {
			t.Run(mux+"/"+mode, func(t *testing.T) { f(t, mux, mode) })
		}
	}
}

var testRings = flag.Int("uring-rings", defaultUringRings, "io_uring reactors per Net in tests and benchmarks")

// newTestNet starts io_uring reactors for one side, stopped when t ends.
func newTestNet(t testing.TB) *Net {
	skipNoUring(t)
	n, err := NewUringNet(*testRings)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Close)
	return n
}

var uringErr = sync.OnceValue(func() error {
	n, err := NewUringNet(1)
	n.Close()
	return err
})

func skipNoUring(t testing.TB) {
	if err := uringErr(); err != nil {
		t.Skip("io_uring unavailable:", err)
	}
}

// socksDial does a real SOCKS5 CONNECT like any application would.
func socksDial(t testing.TB, nw *Net, proxy, target string) net.Conn {
	c, err := nw.Dial(proxy, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	host, portStr, _ := net.SplitHostPort(target)
	port, _ := strconv.Atoi(portStr)
	req := append([]byte{5, 1, 0, 5, 1, 0, 1}, net.ParseIP(host).To4()...)
	req = append(req, byte(port>>8), byte(port))
	c.Write(req)
	resp := make([]byte, 12)
	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(c, resp); err != nil || resp[3] != 0 {
		t.Fatalf("socks connect failed: %v %v", err, resp)
	}
	c.SetReadDeadline(time.Time{})
	return c
}

// roundTrip sends data through the echo target and checks it comes back
// byte-for-byte. pace > 0 sends slowly in 4 KiB pieces.
func roundTrip(t testing.TB, c net.Conn, data []byte, pace time.Duration) {
	go func() {
		for off := 0; off < len(data); {
			n := len(data) - off
			if pace > 0 {
				n = min(n, 4096)
				time.Sleep(pace)
			}
			if _, err := c.Write(data[off : off+n]); err != nil {
				t.Errorf("local write failed: %v", err)
				return
			}
			off += n
		}
	}()
	got := make([]byte, len(data))
	c.SetReadDeadline(time.Now().Add(60 * time.Second))
	if n, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("local connection broke after %d/%d bytes: %v", n, len(data), err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data corrupted (lost, duplicated or reordered bytes)")
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func TestSurvivesRepeatedTunnelKills(t *testing.T) {
	eachMux(t, func(t *testing.T, mux, mode string) {
		e := setup(t, Config{Heartbeat: time.Second, Grace: 10 * time.Second}, mux, mode)
		c := e.dial(t)
		defer c.Close()

		stop := make(chan struct{})
		var kills atomic.Int32
		go func() {
			for {
				select {
				case <-stop:
					return
				case <-time.After(15 * time.Millisecond):
					e.relay.Kill()
					kills.Add(1)
				}
			}
		}()
		roundTrip(t, c, randBytes(64<<20), 0)
		close(stop)

		attaches := e.attaches()
		t.Logf("64 MiB echoed intact across %d tunnel kills (%d tunnel connections used)", kills.Load(), attaches)
		if attaches < 3 {
			t.Fatalf("tunnel was not actually interrupted (attaches=%d)", attaches)
		}
	})
}

func TestSurvivesSilentBlackhole(t *testing.T) {
	eachMux(t, func(t *testing.T, mux, mode string) {
		e := setup(t, Config{Heartbeat: 50 * time.Millisecond, Timeout: 200 * time.Millisecond, Grace: 10 * time.Second}, mux, mode)
		c := e.dial(t)
		defer c.Close()

		time.AfterFunc(300*time.Millisecond, e.relay.Freeze) // no FIN/RST, packets just vanish
		roundTrip(t, c, randBytes(800<<10), 5*time.Millisecond)

		if a := e.attaches(); a < 2 {
			t.Fatalf("heartbeat did not detect the dead tunnel (attaches=%d)", a)
		}
	})
}

func TestMultipleStreamsSurvive(t *testing.T) {
	eachMux(t, func(t *testing.T, mux, mode string) {
		e := setup(t, Config{Heartbeat: time.Second, Grace: 10 * time.Second}, mux, mode)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			c := e.dial(t)
			defer c.Close()
			wg.Add(1)
			go func() { defer wg.Done(); roundTrip(t, c, randBytes(2<<20), time.Millisecond) }()
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		for {
			select {
			case <-done:
				return
			case <-time.After(40 * time.Millisecond):
				e.relay.Kill()
			}
		}
	})
}

func TestGraceExpiryClosesLocalThenRecovers(t *testing.T) {
	eachMux(t, func(t *testing.T, mux, mode string) {
		e := setup(t, Config{Heartbeat: 50 * time.Millisecond, Grace: 300 * time.Millisecond}, mux, mode)
		c := e.dial(t)
		defer c.Close()
		roundTrip(t, c, []byte("hello"), 0)

		e.relay.SetDown(true)
		start := time.Now()
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := c.Read(make([]byte, 1)); err == nil || time.Since(start) > 3*time.Second {
			t.Fatalf("local connection should close once grace expires (err=%v after %v)", err, time.Since(start))
		}
		if time.Since(start) < 250*time.Millisecond {
			t.Fatal("local connection closed before the grace period")
		}

		e.relay.SetDown(false)
		c2 := e.dial(t) // a fresh session is created transparently
		defer c2.Close()
		roundTrip(t, c2, []byte("back again"), 0)
	})
}

// Data then FIN: the app must get every byte and then EOF, never an early
// EOF. The echo target closes as soon as it echoed everything, so its FIN
// races the last data frames through the proxy.
func TestHalfCloseAfterBurst(t *testing.T) {
	eachMux(t, func(t *testing.T, mux, mode string) {
		if mux == "smux" {
			// smux v1.5.57: once both sides sent FIN, streamClosed calls
			// recycleTokens, which drops data still buffered but unread.
			t.Skip("known smux bug: half-closing both ways drops unread data")
		}
		e := setup(t, Config{Heartbeat: time.Second, Grace: 10 * time.Second}, mux, mode)
		c := e.dial(t)
		defer c.Close()
		data := randBytes(4 << 20)
		go func() {
			if _, err := c.Write(data); err != nil {
				t.Errorf("write: %v", err)
			}
			c.(interface{ CloseWrite() error }).CloseWrite()
		}()
		c.SetReadDeadline(time.Now().Add(30 * time.Second))
		got, err := io.ReadAll(c)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("got %d bytes, %v; want all %d bytes, then EOF", len(got), err, len(data))
		}
	})
}

// A stream whose app stops reading must not block the others. This is what
// per-stream flow control buys, and why smux and yamux exist.
func TestSlowStreamDoesNotStallOthers(t *testing.T) {
	eachMux(t, func(t *testing.T, mux, mode string) {
		if mux == "custom" {
			t.Skip("known limitation: mux.go has no per-stream flow control")
		}
		e := setup(t, Config{Heartbeat: time.Second, Grace: 10 * time.Second}, mux, mode)
		slow := e.dial(t)
		defer slow.Close()
		go slow.Write(randBytes(64 << 20)) // echoed back, but never read
		time.Sleep(time.Second)            // let it fill every buffer on its path

		fast := e.dial(t)
		defer fast.Close()
		roundTrip(t, fast, randBytes(1<<20), 0)
	})
}

// BenchmarkThroughput echoes 8 MiB per iteration through the whole proxy, for
// every multiplexer and socket mode (see setup), with a direct TCP connection
// to the same echo server as the baseline:
//
//	go test -run '^$' -bench Throughput
func BenchmarkThroughput(b *testing.B) {
	data := randBytes(8 << 20)
	run := func(b *testing.B, c net.Conn) {
		defer c.Close()
		b.SetBytes(int64(len(data)))
		for b.Loop() {
			roundTrip(b, c, data, 0)
		}
	}
	b.Run("direct", func(b *testing.B) {
		c, err := net.Dial("tcp", startEcho(b, nil))
		if err != nil {
			b.Fatal(err)
		}
		run(b, c)
	})
	for _, mux := range muxes {
		for _, mode := range []string{"std", "uring", "uring-all"} {
			b.Run(mux+"/"+mode, func(b *testing.B) {
				e := setup(b, Config{}, mux, mode)
				run(b, e.dial(b))
				if mode != "std" { // batching: socket ops per io_uring_enter, proxy only
					var ops, enters uint64
					for _, n := range e.proxy {
						o, en := n.Stats()
						ops, enters = ops+o, enters+en
					}
					b.ReportMetric(float64(ops)/float64(enters), "ops/enter")
				}
			})
		}
	}
}
