package main

// The same job as mux.go, done by two widely used libraries, for comparison:
// smux (github.com/xtaci/smux) and yamux (github.com/hashicorp/yamux). Pick
// one with -mux on both sides. They run over the same resumable Session, so
// they never see a reconnect either. What they add over mux.go is per-stream
// flow control: a stream whose reader stalls no longer stalls the others.
//
// The libraries only carry bytes, so each stream starts with a tiny header:
//
//	client -> server: len u16 | "host:port"
//	server -> client: status u8 (0 = connected)
//
// Both libraries assume the connection below them is live, so their
// keepalives and timeouts would kill the whole mux during a tunnel outage.
// The Session already detects dead tunnels and gives up after Grace, so the
// configs below switch those off or raise them.

import (
	"encoding/binary"
	"io"
	"net"

	"github.com/hashicorp/yamux"
	"github.com/xtaci/smux"
)

func smuxConfig() *smux.Config {
	c := smux.DefaultConfig()
	c.Version = 2 // v2 adds per-stream flow control; v1 has mux.go's head-of-line problem
	c.KeepAliveDisabled = true
	// Not configurable: opening or closing a stream gives up after 30 s if the
	// Session's retransmit buffer stays full that long.
	return c
}

func yamuxConfig(cfg Config) *yamux.Config {
	c := yamux.DefaultConfig()
	c.EnableKeepAlive = false
	// A write may wait out a whole outage: Timeout to notice it, then Grace.
	c.ConnectionWriteTimeout = 2 * (cfg.Timeout + cfg.Grace)
	c.StreamOpenTimeout = 0 // otherwise an open left unacknowledged by an outage closes the whole mux
	return c
}

// libMux hides the small API differences between smux and yamux sessions.
type libMux struct {
	open, accept func() (net.Conn, error)
	close        func() error
}

func newLibMux(mux string, s *Session, client bool) libMux {
	if mux == "smux" {
		newSession := smux.Server
		if client {
			newSession = smux.Client
		}
		ms := must(newSession(s, smuxConfig()))
		return libMux{
			open:   func() (net.Conn, error) { return ms.OpenStream() },
			accept: func() (net.Conn, error) { return ms.AcceptStream() },
			close:  ms.Close,
		}
	}
	newSession := yamux.Server
	if client {
		newSession = yamux.Client
	}
	ms := must(newSession(s, yamuxConfig(s.cfg)))
	return libMux{open: ms.Open, accept: ms.Accept, close: ms.Close}
}

// must unwraps a constructor that fails only on an invalid config; ours are fixed.
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// libClient opens one library stream per SOCKS connection.
type libClient struct {
	open func() (net.Conn, error)
}

func newLibClient(mux string, s *Session) *libClient {
	lm := newLibMux(mux, s, true)
	// The server never opens streams, so accept returns only once the Session
	// is dead for good (or the server misbehaves). Closing the mux then frees
	// its goroutines; smux would not do that by itself.
	go func() {
		lm.accept()
		lm.close()
	}()
	return &libClient{open: lm.open}
}

// Open sends the target, waits for the server's verdict, then joins local to
// the stream. Unlike mux.go's Open it blocks for the stream's whole life,
// which is fine because handleSocks runs on its own goroutine.
func (lc *libClient) Open(addr string, local net.Conn, reply func(ok bool)) error {
	st, err := lc.open()
	if err != nil {
		return err
	}
	defer st.Close()
	var status [1]byte
	_, err = st.Write(append(binary.BigEndian.AppendUint16(nil, uint16(len(addr))), addr...))
	if err == nil {
		_, err = io.ReadFull(st, status[:])
	}
	if err != nil {
		return err
	}
	defer local.Close()
	reply(status[0] == 0)
	if status[0] == 0 {
		join(local, st)
	}
	return nil
}

// serveLib is the server side: it serves streams until the Session dies for good.
func serveLib(mux string, s *Session, dial func(string) (net.Conn, error)) {
	lm := newLibMux(mux, s, false)
	defer lm.close()
	for {
		st, err := lm.accept()
		if err != nil {
			return
		}
		go serveStream(st, dial)
	}
}

// serveStream reads the target from the stream header, dials it, reports the
// result and joins the stream to the target.
func serveStream(st net.Conn, dial func(string) (net.Conn, error)) {
	defer st.Close()
	var n [2]byte
	if _, err := io.ReadFull(st, n[:]); err != nil {
		return
	}
	addr := make([]byte, binary.BigEndian.Uint16(n[:]))
	if _, err := io.ReadFull(st, addr); err != nil {
		return
	}
	c, err := dial(string(addr))
	if err != nil {
		st.Write([]byte{1})
		return
	}
	defer c.Close()
	if _, err := st.Write([]byte{0}); err == nil {
		join(c, st)
	}
}

// join copies both ways until both directions are done.
func join(a, b net.Conn) {
	done := make(chan struct{})
	go func() {
		pipe(a, b)
		close(done)
	}()
	pipe(b, a)
	<-done
}

// pipe copies src to dst. A clean EOF is passed on as a half-close, like
// mux.go's mFin; an error tears down both sides, which also ends the other
// pipe. yamux streams have no CloseWrite, but their Close is a half-close.
func pipe(dst, src net.Conn) {
	if _, err := io.Copy(dst, src); err != nil {
		dst.Close()
		src.Close()
		return
	}
	if cw, ok := dst.(interface{ CloseWrite() error }); ok {
		cw.CloseWrite()
	} else {
		dst.Close()
	}
}
