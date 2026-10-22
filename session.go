package main

// Resumable session: a reliable byte stream that outlives the TCP connection
// carrying it.
//
// Each side counts the bytes it has sent and received. Sent bytes stay in a
// retransmit buffer until the peer acknowledges them. When the tunnel TCP
// connection dies, the session is merely "detached": Read and Write keep
// blocking (which backpressures the local sockets) until a new TCP connection
// is attached. On attach both sides exchange their receive offsets and replay
// everything after the peer's offset, so the receiver never sees a byte twice
// or a gap.
//
// Why this is needed even though TCP is reliable: TCP only guarantees
// delivery while the connection lives. When it dies, bytes our kernel
// accepted may never have reached the peer application, and we cannot tell
// how many. Byte offsets at the application layer answer that question.
//
// README.md has the offset diagram, the reconnect sequence and the wire format.
//
// PoC: no encryption or authentication of the tunnel itself. Run it inside
// your real transport (TLS etc).

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

const (
	// Frames sent both ways once a tunnel is attached.
	frData = iota // 0 | len u16 | payload
	frAck         // 1 | recvOff u64
	frPing        // 2

	maxChunk = 16 << 10
	helloLen = 4 + 16 + 16 + 8 // "RSP1" | id | token | recvOff u64
)

var (
	errRejected = errors.New("session rejected by server (expired or unknown)")
	errExpired  = errors.New("session expired: tunnel not restored within grace period")
)

type Config struct {
	Heartbeat time.Duration // ping interval
	Timeout   time.Duration // tunnel is declared dead after this much silence
	Grace     time.Duration // how long a detached session is kept alive
	MaxBuf    int           // max buffered bytes per direction (backpressure limit)
	Logf      func(format string, args ...any)
}

func (c Config) withDefaults() Config {
	if c.Heartbeat == 0 {
		c.Heartbeat = 5 * time.Second
	}
	if c.Timeout == 0 {
		c.Timeout = 3 * c.Heartbeat
	}
	if c.Grace == 0 {
		c.Grace = 60 * time.Second
	}
	if c.MaxBuf == 0 {
		c.MaxBuf = 4 << 20
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
	return c
}

// Session state is guarded by mu. Every goroutine that waits for a state
// change (Read, Write, the tunnel loops) sleeps on cond, and every change
// calls cond.Broadcast. One mutex plus one condition variable keeps the
// concurrency easy to reason about: all decisions happen under the same lock.
type Session struct {
	cfg   Config
	id    [16]byte // picks the session on the server when the client reconnects
	token [16]byte // secret proving the reconnecting client owns that session

	mu   sync.Mutex
	cond *sync.Cond

	// send side: sendBuf holds bytes [sendBase, sendBase+len), i.e. everything
	// not yet acknowledged. sendNext is the next offset to put on the wire.
	// Both buffers are bytes.Buffer, not plain slices, because it reuses its
	// array as bytes are consumed from the front instead of reallocating.
	sendBuf  bytes.Buffer
	sendBase uint64
	sendNext uint64
	ackDue   bool // we received data the peer has not heard an ACK for yet
	pingDue  bool // heartbeat tick: send something so the peer's read deadline resets

	// receive side
	recvBuf bytes.Buffer
	recvOff uint64 // total bytes received in order

	conn net.Conn // current tunnel, nil while detached
	// gen is bumped on every attach/detach. Each tunnel goroutine remembers
	// the gen it started with and quits once it changes, so goroutines of an
	// old, dead connection can never touch the state of the new one.
	gen      uint64
	attaches int
	err      error // set once the session is dead for good
}

func newSession(cfg Config, id, token [16]byte) *Session {
	s := &Session{cfg: cfg, id: id, token: token}
	s.cond = sync.NewCond(&s.mu)
	s.detachLocked() // arms the grace timer for a session that never attaches
	return s
}

// Read returns in-order bytes from the peer. Blocks across tunnel outages.
func (s *Session) Read(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.recvBuf.Len() == 0 {
		if s.err != nil {
			return 0, s.err
		}
		s.cond.Wait()
	}
	n, _ := s.recvBuf.Read(p)
	s.cond.Broadcast() // room freed: wakes a readLoop blocked on a full recvBuf
	return n, nil
}

// Write queues p for delivery. It blocks while the retransmit buffer is full,
// which is what pauses the local sockets during an outage. p is appended in
// one piece, so concurrent Writes never interleave.
func (s *Session) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// An empty buffer always accepts p, even one larger than MaxBuf; otherwise
	// such a write would wait forever.
	for s.err == nil && s.sendBuf.Len() > 0 && s.sendBuf.Len()+len(p) > s.cfg.MaxBuf {
		s.cond.Wait()
	}
	if s.err != nil {
		return 0, s.err
	}
	s.sendBuf.Write(p)
	s.cond.Broadcast() // wakes writeLoop
	return len(p), nil
}

func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *Session) Attaches() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attaches
}

// Close kills the session for good. smux and yamux call it when they shut down.
func (s *Session) Close() error {
	s.closeWith(net.ErrClosed)
	return nil
}

// closeWith kills the session for good; the first error wins.
func (s *Session) closeWith(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
		s.detachLocked()
	}
	s.mu.Unlock()
}

// detachLocked drops the current tunnel (if any), invalidates its goroutines
// and arms the grace timer.
func (s *Session) detachLocked() {
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	s.gen++
	g := s.gen
	if s.err == nil {
		// The timer expires the session only if nothing happened since: any
		// attach changes gen, which makes this timer a no-op.
		time.AfterFunc(s.cfg.Grace, func() {
			s.mu.Lock()
			expired := s.gen == g && s.err == nil
			s.mu.Unlock()
			if expired {
				s.cfg.Logf("session %x: grace period over, giving up", s.id[:4])
				s.closeWith(errExpired)
			}
		})
	}
	s.cond.Broadcast() // wakes old tunnel goroutines so they see the new gen and exit
}

// quiesce detaches and returns the receive offset, which is now stable
// because no reader is running.
func (s *Session) quiesce() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.detachLocked()
	}
	return s.recvOff
}

// drop is called by a tunnel goroutine when its connection fails. The gen
// check ignores late failures from a connection that was already replaced.
func (s *Session) drop(gen uint64) {
	s.mu.Lock()
	if s.gen == gen && s.err == nil {
		s.cfg.Logf("session %x: tunnel lost, %d bytes unacked, local connections kept", s.id[:4], s.sendBuf.Len())
		s.detachLocked()
	}
	s.mu.Unlock()
}

func (s *Session) waitDetached() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.conn != nil && s.err == nil {
		s.cond.Wait()
	}
	return s.err
}

// ackToLocked frees every byte below off: the peer has it, so it will never
// need a retransmit. Offsets outside the buffer are stale or bogus and ignored.
func (s *Session) ackToLocked(off uint64) {
	if off <= s.sendBase || off > s.sendBase+uint64(s.sendBuf.Len()) {
		return
	}
	s.sendBuf.Next(int(off - s.sendBase))
	s.sendBase = off
	if s.sendNext < off {
		s.sendNext = off
	}
	s.cond.Broadcast() // room freed: wakes a blocked Write
}

// attach installs a fresh tunnel. peerRecv is how many of our bytes the peer
// has received; transmission restarts exactly there.
func (s *Session) attach(c net.Conn, peerRecv uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		c.Close()
		return s.err
	}
	// We can only resume if we still hold every byte the peer is missing.
	if peerRecv < s.sendBase || peerRecv > s.sendBase+uint64(s.sendBuf.Len()) {
		c.Close()
		return errors.New("peer resume offset is outside the retransmit buffer")
	}
	if s.conn != nil { // two handshakes raced on the server: the newest one wins
		s.conn.Close()
	}
	s.gen++ // retires the old tunnel goroutines and any pending grace timer
	s.ackToLocked(peerRecv)
	s.sendNext = peerRecv // rewind: resend everything the peer has not received
	s.conn = c
	s.attaches++
	s.ackDue, s.pingDue = true, false
	s.cfg.Logf("session %x: tunnel attached (#%d), replaying %d bytes", s.id[:4], s.attaches, s.sendBuf.Len())
	gen := s.gen
	go s.readLoop(c, gen)
	go s.writeLoop(c, gen)
	go s.heartbeat(gen)
	s.cond.Broadcast()
	return nil
}

// readLoop parses frames from one tunnel connection until it fails or is
// replaced.
func (s *Session) readLoop(c net.Conn, gen uint64) {
	defer s.drop(gen)
	br := bufio.NewReaderSize(c, 64<<10)
	var hdr [9]byte
	buf := make([]byte, maxChunk)
	for {
		// The peer sends at least a PING every Heartbeat, so a read that
		// waits longer than Timeout means the path is silently dead (no
		// FIN or RST ever arrives, e.g. a middlebox dropping packets).
		c.SetReadDeadline(time.Now().Add(s.cfg.Timeout))
		if _, err := io.ReadFull(br, hdr[:1]); err != nil {
			return
		}
		switch hdr[0] {
		case frPing: // nothing to do: receiving it already reset the deadline
		case frAck:
			if _, err := io.ReadFull(br, hdr[1:9]); err != nil {
				return
			}
			s.mu.Lock()
			if s.gen == gen {
				s.ackToLocked(binary.BigEndian.Uint64(hdr[1:9]))
			}
			s.mu.Unlock()
		case frData:
			if _, err := io.ReadFull(br, hdr[1:3]); err != nil {
				return
			}
			n := int(binary.BigEndian.Uint16(hdr[1:3]))
			if n == 0 || n > maxChunk {
				return
			}
			if _, err := io.ReadFull(br, buf[:n]); err != nil {
				return
			}
			s.mu.Lock()
			// Receive-side backpressure: wait for Read to drain recvBuf.
			// Meanwhile we stop reading the socket, so TCP flow control
			// slows the peer down.
			for s.gen == gen && s.recvBuf.Len() >= s.cfg.MaxBuf {
				s.cond.Wait()
			}
			if s.gen != gen { // tunnel was replaced: peer will resend this
				s.mu.Unlock()
				return
			}
			s.recvBuf.Write(buf[:n])
			s.recvOff += uint64(n)
			s.ackDue = true
			s.cond.Broadcast()
			s.mu.Unlock()
		default:
			return
		}
	}
}

// writeLoop is the only writer of one tunnel connection. Each round it packs
// an ACK (if due) and up to maxChunk of unsent data into one write; if there
// is neither, it sends a PING.
func (s *Session) writeLoop(c net.Conn, gen uint64) {
	defer s.drop(gen)
	out := make([]byte, 0, maxChunk+16)
	for {
		s.mu.Lock()
		for s.gen == gen && !s.ackDue && !s.pingDue && s.sendNext == s.sendBase+uint64(s.sendBuf.Len()) {
			s.cond.Wait()
		}
		if s.gen != gen {
			s.mu.Unlock()
			return
		}
		out = out[:0]
		if s.ackDue {
			out = append(out, frAck)
			out = binary.BigEndian.AppendUint64(out, s.recvOff)
			s.ackDue = false
		}
		if pending := s.sendBuf.Bytes()[s.sendNext-s.sendBase:]; len(pending) > 0 {
			pending = pending[:min(len(pending), maxChunk)]
			out = append(out, frData, byte(len(pending)>>8), byte(len(pending)))
			out = append(out, pending...)
			// sendNext moves forward, but the bytes stay in sendBuf until ACKed.
			s.sendNext += uint64(len(pending))
		}
		if len(out) == 0 {
			out = append(out, frPing)
		}
		s.pingDue = false
		s.mu.Unlock()

		// Write without holding the lock: it may block for up to Timeout.
		c.SetWriteDeadline(time.Now().Add(s.cfg.Timeout))
		if _, err := c.Write(out); err != nil {
			return
		}
	}
}

// heartbeat sets pingDue every Heartbeat, so writeLoop sends at least that
// often and the peer's read deadline never fires on a healthy but idle link.
func (s *Session) heartbeat(gen uint64) {
	t := time.NewTicker(s.cfg.Heartbeat)
	defer t.Stop()
	for range t.C {
		s.mu.Lock()
		if s.gen != gen {
			s.mu.Unlock()
			return
		}
		s.pingDue = true
		s.cond.Broadcast()
		s.mu.Unlock()
	}
}

// ---------------------------------------------------------------- client side

// DialSession returns immediately; a background loop keeps the session
// attached to addr, redialing whenever the tunnel drops.
func DialSession(addr string, cfg Config) *Session {
	cfg = cfg.withDefaults()
	var id, token [16]byte
	rand.Read(id[:])
	rand.Read(token[:])
	s := newSession(cfg, id, token)
	go s.dialLoop(addr)
	return s
}

// dialLoop reconnects with exponential backoff (50ms, doubling, capped at
// 2s) until the session dies. A rejection is final: the server no longer
// knows the session, so retrying cannot help.
func (s *Session) dialLoop(addr string) {
	const minBackoff = 50 * time.Millisecond
	backoff := minBackoff
	for {
		if s.waitDetached() != nil {
			return
		}
		switch err := s.connect(addr); {
		case err == nil:
			backoff = minBackoff
		case errors.Is(err, errRejected):
			s.closeWith(err)
			return
		default:
			time.Sleep(backoff)
			backoff = min(backoff*2, 2*time.Second)
		}
	}
}

// connect does the client half of the handshake: send hello with our
// recvOff, read the server's recvOff, then attach with it.
func (s *Session) connect(addr string) error {
	c, err := net.DialTimeout("tcp", addr, s.cfg.Timeout)
	if err != nil {
		return err
	}
	c.SetDeadline(time.Now().Add(s.cfg.Timeout))
	hello := make([]byte, 0, helloLen)
	hello = append(hello, "RSP1"...)
	hello = append(hello, s.id[:]...)
	hello = append(hello, s.token[:]...)
	s.mu.Lock()
	hello = binary.BigEndian.AppendUint64(hello, s.recvOff) // stable: we are detached
	s.mu.Unlock()
	var reply [9]byte // status u8 (0 ok) | server's recvOff u64
	if _, err = c.Write(hello); err == nil {
		_, err = io.ReadFull(c, reply[:])
	}
	if err != nil {
		c.Close()
		return err
	}
	if reply[0] != 0 {
		c.Close()
		return errRejected
	}
	c.SetDeadline(time.Time{})
	if err := s.attach(c, binary.BigEndian.Uint64(reply[1:])); err != nil {
		s.closeWith(err)
		return errRejected
	}
	return nil
}

// ---------------------------------------------------------------- server side

type Server struct {
	Cfg     Config
	Handler func(*Session) // called once per new session; session is removed when it returns

	mu       sync.Mutex
	sessions map[[16]byte]*Session
}

func (sv *Server) Serve(ln net.Listener) error {
	sv.Cfg = sv.Cfg.withDefaults()
	sv.sessions = map[[16]byte]*Session{}
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go sv.handshake(c)
	}
}

// handshake either creates a new session or resumes a known one.
func (sv *Server) handshake(c net.Conn) {
	c.SetDeadline(time.Now().Add(sv.Cfg.Timeout))
	var hello [helloLen]byte
	if _, err := io.ReadFull(c, hello[:]); err != nil || string(hello[:4]) != "RSP1" {
		c.Close()
		return
	}
	var id, token [16]byte
	copy(id[:], hello[4:20])
	copy(token[:], hello[20:36])
	peerRecv := binary.BigEndian.Uint64(hello[36:])

	sv.mu.Lock()
	s, isNew := sv.sessions[id], false
	switch {
	case s == nil && peerRecv == 0: // brand new session: nothing received yet
		s, isNew = newSession(sv.Cfg, id, token), true
		sv.sessions[id] = s
	// Unknown id with peerRecv > 0 means the session expired here (or the
	// server restarted): the data is lost, so refuse instead of silently
	// starting over. ConstantTimeCompare avoids leaking the token via timing.
	case s == nil, subtle.ConstantTimeCompare(s.token[:], token[:]) != 1, s.Err() != nil:
		sv.mu.Unlock()
		c.Write([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0})
		c.Close()
		return
	}
	sv.mu.Unlock()

	// Detach first so recvOff stops moving, then tell the client where to
	// resume. Otherwise the old tunnel could still deliver bytes after we
	// reported the offset, and the client would send them again.
	reply := binary.BigEndian.AppendUint64([]byte{0}, s.quiesce())
	if _, err := c.Write(reply); err != nil {
		c.Close()
		return
	}
	c.SetDeadline(time.Time{})
	if err := s.attach(c, peerRecv); err != nil {
		s.closeWith(err)
	}
	if isNew {
		go func() {
			sv.Handler(s)
			sv.mu.Lock()
			delete(sv.sessions, id)
			sv.mu.Unlock()
		}()
	}
}
