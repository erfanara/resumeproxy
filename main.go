// resumeproxy: PoC of a SOCKS5 proxy whose client<->server tunnel survives
// TCP connection loss without closing local connections. README.md has usage
// and the design; the code is layered bottom-up:
//
//	uring_linux.go  tunnel I/O through io_uring instead of Go's netpoller (-io)
//	session.go      Session: one reliable byte stream that survives tunnel reconnects
//	mux.go          Mux: many proxied connections ("streams") over one Session
//	libmux.go       the same job done by smux or yamux, for comparison (-mux)
//	socks.go        SOCKS5 front end (client)
//	main.go         Client session holder, target dialer (server), flags
package main

import (
	"flag"
	"log"
	"net"
	"sync"
	"time"
)

// Tunnel carries proxied connections over a Session: mux.go's Mux, or smux and
// yamux through libmux.go. reply reports whether the server reached addr.
type Tunnel interface {
	Open(addr string, local net.Conn, reply func(ok bool)) error
}

// Client owns the current session and replaces it if it ever dies for good
// (the grace period expired). Short outages are hidden by the Session itself;
// the Client only ever sees a session that is alive or permanently dead.
type Client struct {
	Server string
	Mux    string // "custom" (mux.go), "smux" or "yamux"; must match the server
	Cfg    Config

	mu   sync.Mutex
	sess *Session
	tun  Tunnel
}

// tunnel returns the live multiplexer, creating a fresh session if needed.
// The multiplexer of a dead session closes its streams, so new SOCKS
// connections simply land on the new one.
func (cl *Client) tunnel() Tunnel {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.sess == nil || cl.sess.Err() != nil {
		cl.sess = DialSession(cl.Server, cl.Cfg)
		if cl.Mux == "smux" || cl.Mux == "yamux" {
			cl.tun = newLibClient(cl.Mux, cl.sess)
		} else {
			m := NewMux(cl.sess, nil)
			go m.Run()
			cl.tun = m
		}
	}
	return cl.tun
}

func dialTarget(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, 10*time.Second)
}

// serverHandler returns the proxy-server side of one session: a multiplexer
// that dials whatever target the client asks for.
func serverHandler(mux string) func(*Session) {
	return func(s *Session) {
		if mux == "smux" || mux == "yamux" {
			serveLib(mux, s)
		} else {
			NewMux(s, dialTarget).Run()
		}
	}
}

func main() {
	mode := flag.String("mode", "", "client or server")
	listen := flag.String("listen", "127.0.0.1:1080", "listen address (SOCKS5 for client, tunnel for server)")
	server := flag.String("server", "", "proxy server address (client mode)")
	mux := flag.String("mux", "custom", "stream multiplexer: custom (mux.go), smux or yamux; must match on both sides")
	def := Config{}.withDefaults()
	grace := flag.Duration("grace", def.Grace, "how long to keep a session while the tunnel is down")
	hb := flag.Duration("heartbeat", def.Heartbeat, "heartbeat interval; tunnel is dead after 3 missed")
	bufMB := flag.Int("buf", def.MaxBuf>>20, "retransmit buffer per direction, MiB")
	ioMode := flag.String("io", "std", "tunnel I/O: std (Go netpoller) or uring (io_uring, Linux 5.6+)")
	flag.Parse()

	if *mux != "custom" && *mux != "smux" && *mux != "yamux" {
		log.Fatal("-mux must be custom, smux or yamux")
	}
	if *ioMode != "std" && *ioMode != "uring" {
		log.Fatal("-io must be std or uring")
	}
	if *ioMode == "uring" {
		if err := uringProbe(); err != nil { // fail now, not on every reconnect
			log.Fatal("-io uring: ", err)
		}
	}
	cfg := Config{Heartbeat: *hb, Grace: *grace, MaxBuf: *bufMB << 20, Uring: *ioMode == "uring", Logf: log.Printf}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	switch *mode {
	case "server":
		log.Printf("tunnel server on %s (mux: %s, io: %s)", ln.Addr(), *mux, *ioMode)
		log.Fatal((&Server{Cfg: cfg, Handler: serverHandler(*mux)}).Serve(ln))
	case "client":
		if *server == "" {
			log.Fatal("-server is required")
		}
		log.Printf("SOCKS5 on %s -> %s (mux: %s, io: %s)", ln.Addr(), *server, *mux, *ioMode)
		log.Fatal((&Client{Server: *server, Mux: *mux, Cfg: cfg}).ServeSocks(ln))
	default:
		log.Fatal("-mode must be client or server")
	}
}
