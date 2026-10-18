package main

import (
	"bytes"
	"crypto/rand"
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

func newRelay(t testing.TB, upstream string) *relay {
	r := &relay{ln: listen(t), upstream: upstream, conns: map[*relayConn]bool{}}
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
			b, err := net.Dial("tcp", upstream)
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

func listen(t testing.TB) net.Listener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return ln
}

type env struct {
	client *Client
	relay  *relay
	socks  string
	echo   string
}

// startEcho runs a target server that sends back everything it receives.
func startEcho(t testing.TB) string {
	ln := listen(t)
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

func setup(t testing.TB, cfg Config, mux string) *env {
	cfg.Logf = t.Logf
	srvLn := listen(t)
	go (&Server{Cfg: cfg, Handler: serverHandler(mux)}).Serve(srvLn)
	r := newRelay(t, srvLn.Addr().String())
	cl := &Client{Server: r.ln.Addr().String(), Mux: mux, Cfg: cfg}
	socksLn := listen(t)
	go cl.ServeSocks(socksLn)
	return &env{client: cl, relay: r, socks: socksLn.Addr().String(), echo: startEcho(t)}
}

// attaches reports how many tunnel connections the current session has used.
func (e *env) attaches() int {
	e.client.mu.Lock()
	defer e.client.mu.Unlock()
	return e.client.sess.Attaches()
}

var muxes = []string{"custom", "smux", "yamux"}

// eachMux runs f once per multiplexer; the resumable Session below is the same.
func eachMux(t *testing.T, f func(t *testing.T, mux string)) {
	for _, mux := range muxes {
		t.Run(mux, func(t *testing.T) { f(t, mux) })
	}
}

// socksDial does a real SOCKS5 CONNECT like any application would.
func socksDial(t testing.TB, proxy, target string) net.Conn {
	c, err := net.Dial("tcp", proxy)
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
	eachMux(t, func(t *testing.T, mux string) {
		e := setup(t, Config{Heartbeat: time.Second, Grace: 10 * time.Second}, mux)
		c := socksDial(t, e.socks, e.echo)
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
	eachMux(t, func(t *testing.T, mux string) {
		e := setup(t, Config{Heartbeat: 50 * time.Millisecond, Timeout: 200 * time.Millisecond, Grace: 10 * time.Second}, mux)
		c := socksDial(t, e.socks, e.echo)
		defer c.Close()

		time.AfterFunc(300*time.Millisecond, e.relay.Freeze) // no FIN/RST, packets just vanish
		roundTrip(t, c, randBytes(800<<10), 5*time.Millisecond)

		if a := e.attaches(); a < 2 {
			t.Fatalf("heartbeat did not detect the dead tunnel (attaches=%d)", a)
		}
	})
}

func TestMultipleStreamsSurvive(t *testing.T) {
	eachMux(t, func(t *testing.T, mux string) {
		e := setup(t, Config{Heartbeat: time.Second, Grace: 10 * time.Second}, mux)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			c := socksDial(t, e.socks, e.echo)
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
	eachMux(t, func(t *testing.T, mux string) {
		e := setup(t, Config{Heartbeat: 50 * time.Millisecond, Grace: 300 * time.Millisecond}, mux)
		c := socksDial(t, e.socks, e.echo)
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
		c2 := socksDial(t, e.socks, e.echo) // a fresh session is created transparently
		defer c2.Close()
		roundTrip(t, c2, []byte("back again"), 0)
	})
}

// A stream whose app stops reading must not block the others. This is what
// per-stream flow control buys, and why smux and yamux exist.
func TestSlowStreamDoesNotStallOthers(t *testing.T) {
	eachMux(t, func(t *testing.T, mux string) {
		if mux == "custom" {
			t.Skip("known limitation: mux.go has no per-stream flow control")
		}
		e := setup(t, Config{Heartbeat: time.Second, Grace: 10 * time.Second}, mux)
		slow := socksDial(t, e.socks, e.echo)
		defer slow.Close()
		go slow.Write(randBytes(64 << 20)) // echoed back, but never read
		time.Sleep(time.Second)            // let it fill every buffer on its path

		fast := socksDial(t, e.socks, e.echo)
		defer fast.Close()
		roundTrip(t, fast, randBytes(1<<20), 0)
	})
}

// BenchmarkThroughput echoes 8 MiB per iteration through the whole proxy, with
// a direct TCP connection to the same echo server as the baseline:
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
		c, err := net.Dial("tcp", startEcho(b))
		if err != nil {
			b.Fatal(err)
		}
		run(b, c)
	})
	for _, mux := range muxes {
		b.Run(mux, func(b *testing.B) {
			e := setup(b, Config{}, mux)
			run(b, socksDial(b, e.socks, e.echo))
		})
	}
}
