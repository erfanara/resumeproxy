package main

// Minimal stream multiplexer on top of a Session. It knows nothing about
// reconnects: the session below simply never fails unless the grace period
// runs out. That separation is the point of the design: resumption is solved
// once, for the whole byte stream, instead of per proxied connection.
//
// Frame: streamID u32 | type u8 | len u16 | payload
//
// Each side sends mFin when its local socket hits EOF (TCP half-close); a
// stream is freed once both directions are done. mRst kills it at once.
//
// Every stream has a writer goroutine: Run only queues peer data, so local
// writes of different streams run at the same time, and each write takes
// everything that piled up.
//
// PoC limitation: no per-stream flow control, so one local socket that stops
// reading stalls the other streams of the same session once its queue is
// full (maxStreamBuf). smux and yamux solve this with per-stream windows;
// compare them with -mux (libmux.go).

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
)

const (
	mOpen   = iota // payload: "host:port"
	mOpenOK        // target connected
	mData
	mFin // sender will write no more
	mRst // stream is gone

	mHdrLen      = 7         // streamID u32 | type u8 | len u16
	maxPayload   = 1<<16 - 1 // largest payload the u16 length field allows
	maxStreamBuf = 256 << 10 // peer data queued per stream before Run blocks
)

type stream struct {
	conn       net.Conn
	reply      func(ok bool) // client side, pending until OpenOK/Rst
	localDone  bool          // we sent mFin (our local socket hit EOF)
	remoteDone bool          // the writer passed on the peer's mFin

	// Peer data for the writer goroutine to write to conn.
	mu   sync.Mutex
	cond *sync.Cond
	out  []byte
	fin  bool // peer sent mFin: half-close conn once out is written
	dead bool // reset or session gone: drop out, writer exits
}

func newStream(conn net.Conn, reply func(ok bool)) *stream {
	st := &stream{conn: conn, reply: reply}
	st.cond = sync.NewCond(&st.mu)
	return st
}

// push queues peer data for the writer, blocking while the queue is full.
func (st *stream) push(p []byte) {
	st.mu.Lock()
	for len(st.out) >= maxStreamBuf && !st.dead {
		st.cond.Wait()
	}
	if !st.dead {
		st.out = append(st.out, p...)
		st.cond.Broadcast()
	}
	st.mu.Unlock()
}

// end marks the peer's mFin (fin) or the stream's death (!fin) for the writer.
func (st *stream) end(fin bool) {
	st.mu.Lock()
	if fin {
		st.fin = true
	} else {
		st.dead = true
	}
	st.cond.Broadcast()
	st.mu.Unlock()
}

type Mux struct {
	sess *Session
	dial func(addr string) (net.Conn, error) // server side only; nil on the client

	mu      sync.Mutex
	streams map[uint32]*stream
	nextID  uint32 // only the client allocates IDs, so no clash with the server
	dead    bool
}

func NewMux(s *Session, dial func(string) (net.Conn, error)) *Mux {
	return &Mux{sess: s, dial: dial, streams: map[uint32]*stream{}}
}

// send writes one frame with payload p.
func (m *Mux) send(id uint32, typ byte, p []byte) error {
	return m.sendFrame(id, typ, append(make([]byte, mHdrLen, mHdrLen+len(p)), p...))
}

// sendFrame fills in the header of frame, whose first mHdrLen bytes are
// reserved for it, and writes it. Many pump goroutines call this
// concurrently; no extra lock is needed because Session.Write appends the
// whole frame under its own lock, so frames never interleave. Session.Write
// copies frame, so the caller may reuse it right away.
func (m *Mux) sendFrame(id uint32, typ byte, frame []byte) error {
	binary.BigEndian.PutUint32(frame, id)
	frame[4] = typ
	binary.BigEndian.PutUint16(frame[5:], uint16(len(frame)-mHdrLen))
	_, err := m.sess.Write(frame)
	return err
}

// Open asks the server to connect to addr and relays local to it. reply is
// called exactly once, before any target data is written to local.
func (m *Mux) Open(addr string, local net.Conn, reply func(ok bool)) error {
	m.mu.Lock()
	if m.dead {
		m.mu.Unlock()
		return errors.New("session is dead")
	}
	m.nextID++
	id := m.nextID
	st := newStream(local, reply)
	m.streams[id] = st
	m.mu.Unlock()
	go m.writer(id, st)
	return m.send(id, mOpen, []byte(addr))
}

func (m *Mux) get(id uint32) *stream {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.streams[id]
}

// Run processes incoming frames until the session dies. It is the only
// reader of the session, so frames arrive here in order.
func (m *Mux) Run() error {
	br := bufio.NewReaderSize(m.sess, 64<<10)
	var hdr [mHdrLen]byte
	buf := make([]byte, maxPayload)
	var err error
	for {
		if _, err = io.ReadFull(br, hdr[:]); err != nil {
			break
		}
		id := binary.BigEndian.Uint32(hdr[:])
		p := buf[:binary.BigEndian.Uint16(hdr[5:])]
		if _, err = io.ReadFull(br, p); err != nil {
			break
		}
		switch hdr[4] {
		case mOpen:
			if m.dial != nil { // dialing may be slow: never block the frame loop on it
				go m.accept(id, string(p))
			}
		case mOpenOK:
			// No lock for st.reply: until pump starts, only this goroutine
			// touches it, and `go pump` orders the nil-store before pump's reads.
			if st := m.get(id); st != nil && st.reply != nil {
				st.reply(true)
				st.reply = nil
				go m.pump(id, st)
			}
		case mData:
			// Queue it for the stream's writer and move on. Run blocks only
			// when that stream's queue is full: the "no per-stream flow
			// control" limitation.
			if st := m.get(id); st != nil {
				st.push(p)
			}
		case mFin:
			// The writer half-closes the local socket after writing all data
			// that came before the mFin.
			if st := m.get(id); st != nil {
				st.end(true)
			}
		case mRst:
			m.reset(id, false)
		}
	}
	// The session died for good (grace expired or protocol error): tear
	// down every stream so local apps see their connection close.
	m.sess.closeWith(err)
	m.mu.Lock()
	m.dead = true
	all := m.streams
	m.streams = map[uint32]*stream{}
	m.mu.Unlock()
	for _, st := range all {
		closeStream(st)
	}
	return err
}

// closeStream stops the writer, tells a still-pending SOCKS client
// "refused", then closes.
func closeStream(st *stream) {
	st.end(false)
	if st.reply != nil {
		st.reply(false)
	}
	st.conn.Close()
}

// accept runs on the server for each mOpen.
func (m *Mux) accept(id uint32, addr string) {
	c, err := m.dial(addr)
	if err != nil {
		m.send(id, mRst, nil)
		return
	}
	st := newStream(c, nil)
	m.mu.Lock()
	if m.dead { // session died while we were dialing
		m.mu.Unlock()
		c.Close()
		return
	}
	m.streams[id] = st
	m.mu.Unlock()
	go m.writer(id, st)
	if m.send(id, mOpenOK, nil) == nil {
		m.pump(id, st)
	}
}

// pump copies local socket -> session. During an outage send blocks once the
// retransmit buffer is full, so we stop reading and the app sees backpressure.
func (m *Mux) pump(id uint32, st *stream) {
	// Room for the header in front of the data: no allocation per frame. Each
	// read takes as much as one frame can carry: fewer, bigger operations
	// (about a third of the syscalls of 16 KiB reads, see README).
	buf := make([]byte, mHdrLen+maxPayload)
	for {
		n, err := st.conn.Read(buf[mHdrLen:])
		if n > 0 && m.sendFrame(id, mData, buf[:mHdrLen+n]) != nil {
			return // session dead; Run closes everything
		}
		if err == io.EOF { // clean half-close from the app
			m.send(id, mFin, nil)
			m.finish(id, true)
			return
		}
		if err != nil { // local socket broke or was closed by reset
			m.reset(id, true)
			return
		}
	}
}

// writer writes peer data to the local socket: everything queued, in one
// Write. After the peer's mFin and the data before it, it half-closes the
// socket so the app sees EOF while it can still write its own data.
func (m *Mux) writer(id uint32, st *stream) {
	var spare []byte // swapped with st.out, so Run fills one while we write the other
	for {
		st.mu.Lock()
		for len(st.out) == 0 && !st.fin && !st.dead {
			st.cond.Wait()
		}
		if st.dead {
			st.mu.Unlock()
			return
		}
		if len(st.out) == 0 { // fin, and everything is written
			st.mu.Unlock()
			if cw, ok := st.conn.(interface{ CloseWrite() error }); ok {
				cw.CloseWrite()
			}
			m.finish(id, false)
			return
		}
		data := st.out
		st.out = spare[:0]
		st.cond.Broadcast() // room for Run
		st.mu.Unlock()
		if _, err := st.conn.Write(data); err != nil {
			m.reset(id, true)
			return
		}
		spare = data
	}
}

// finish marks one direction as done; the stream is released when both are.
func (m *Mux) finish(id uint32, local bool) {
	m.mu.Lock()
	st := m.streams[id]
	if st == nil {
		m.mu.Unlock()
		return
	}
	if local {
		st.localDone = true
	} else {
		st.remoteDone = true
	}
	done := st.localDone && st.remoteDone
	if done {
		delete(m.streams, id)
	}
	m.mu.Unlock()
	if done {
		st.conn.Close()
	}
}

// reset kills a stream now. notify: also tell the peer (false when the peer
// is the one who sent mRst). Removing it from the map first makes reset
// idempotent, so the pump and the frame loop can both call it safely.
func (m *Mux) reset(id uint32, notify bool) {
	m.mu.Lock()
	st := m.streams[id]
	delete(m.streams, id)
	m.mu.Unlock()
	if st == nil {
		return
	}
	closeStream(st)
	if notify {
		go m.send(id, mRst, nil) // async: never block the frame loop on a full buffer
	}
}
