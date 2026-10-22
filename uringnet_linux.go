//go:build linux && (amd64 || arm64)

package main

// io_uring for every socket (-io uring), with batching.
//
// A Net runs a few reactors. Each reactor is one ring and one goroutine
// locked to its own OS thread. A goroutine that reads, writes, accepts or
// connects puts a request into a reactor's queue and parks. The reactor
// takes everything queued, writes the SQEs, and makes one io_uring_enter
// for the whole batch; then it hands every completion back to its goroutine.
// Under load many sockets have operations pending at once, so one syscall
// carries many of them. That is where io_uring beats one read(2) or
// write(2) per operation.
//
// Every socket is pinned to one reactor (round-robin) for its whole life.
// Socket setup (socket, bind, listen, setsockopt) uses plain syscalls, and
// host names are resolved with Go's resolver.

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	ringEntries = 256               // SQ size; the CQ is twice as big
	maxInflight = ringEntries - 1   // each request makes at most 2 CQEs, plus the wake-up read
	maxBatch    = ringEntries/2 - 1 // requests per round: up to 2 SQEs each, plus wake-up and cancel
	tagTimeout  = 1 << 62           // user_data bit: CQE of a request's linked timeout
	tagWake     = 1 << 63           // user_data of the eventfd read
	tagCancel   = tagWake | 1       // user_data of the cancel-everything op at stop
	maxIO       = 1 << 30           // largest single recv/send
	dialTimeout = 10 * time.Second  // for Session-less dials (targets)
)

// request is one operation. Every socket owns one request per direction and
// reuses it, so an operation allocates nothing.
type request struct {
	op       uint8
	fd       int32
	ptr      unsafe.Pointer // buffer or sockaddr; also keeps it alive while the kernel uses it
	len      uint32
	off      uint64
	opFlags  uint32
	timeout  time.Duration    // 0: no deadline
	ts       syscall.Timespec // read by the linked timeout
	res      int32            // kernel result, or -errno
	timedOut bool
	cqes     int // CQEs still to come
	done     chan struct{}
}

type reactor struct {
	mu       sync.Mutex
	queue    []*request
	asleep   bool // the reactor may be blocked in io_uring_enter: wake it via efd
	stopping bool

	efdMu sync.RWMutex // efd is closed only under the write lock
	efd   int

	// Owned by the reactor goroutine.
	ring        *ring
	wakeBuf     [8]byte // the in-flight eventfd read writes here
	slots       [maxInflight]*request
	free        []uint32
	inflight    int // requests in the ring
	outstanding int // CQEs still to come, of any kind
	halting     bool

	ops, enters atomic.Uint64
	exited      chan struct{}
}

func startReactor() (*reactor, error) {
	r := &reactor{efd: -1, exited: make(chan struct{})}
	ready := make(chan error)
	go r.run(ready)
	if err := <-ready; err != nil {
		return nil, err
	}
	return r, nil
}

func (r *reactor) run(ready chan<- error) {
	// The ring belongs to this OS thread for good (SINGLE_ISSUER). The
	// thread is never unlocked, so it exits together with the goroutine.
	runtime.LockOSThread()
	defer close(r.exited)
	var err error
	if r.ring, err = newRing(ringEntries, setupSubmitAll|setupSingleIssuer|setupDeferTaskrun); err != nil {
		ready <- err
		return
	}
	defer r.ring.close()
	efd, _, errno := syscall.Syscall(syscall.SYS_EVENTFD2, 0, syscall.O_CLOEXEC, 0)
	if errno != 0 {
		ready <- os.NewSyscallError("eventfd", errno)
		return
	}
	r.efd = int(efd)
	defer func() {
		r.efdMu.Lock()
		syscall.Close(r.efd)
		r.efd = -1
		r.efdMu.Unlock()
	}()
	for i := range maxInflight {
		r.free = append(r.free, uint32(i))
	}
	ready <- nil
	r.armWake()
	r.loop()
}

// loop: take queued requests, write their SQEs, one io_uring_enter for the
// whole batch, hand back every completion. It only blocks in the kernel
// when there is nothing to submit, and only after setting asleep (under the
// queue lock) so that the next submitter wakes it.
func (r *reactor) loop() {
	var batch []*request
	cancelSent := false
	for {
		r.mu.Lock()
		r.halting = r.stopping
		n := len(r.queue)
		if !r.halting {
			n = min(n, maxBatch, maxInflight-r.inflight)
		}
		batch = append(batch[:0], r.queue[:n]...)
		r.queue = append(r.queue[:0], r.queue[n:]...)
		r.asleep = n == 0 && !r.halting
		r.mu.Unlock()

		wait := uint32(0)
		if r.halting {
			for _, q := range batch { // never reached the kernel
				q.res = -int32(syscall.ECANCELED)
				q.done <- struct{}{}
			}
			if !cancelSent { // cancel everything in flight, the wake-up read too
				s := r.ring.sqe()
				s.opcode, s.fd, s.opFlags, s.userData = opAsyncCancel, -1, cancelAny|cancelAll, tagCancel
				r.outstanding++
				cancelSent = true
			}
			if r.outstanding == 0 {
				return
			}
			wait = 1
		} else {
			for _, q := range batch {
				r.prep(q)
			}
			if n == 0 {
				wait = 1
			}
		}
		if err := r.ring.enter(wait); err != nil {
			panic("resumeproxy: io_uring reactor: " + err.Error())
		}
		r.enters.Add(1)
		r.ring.reap(r.complete)
	}
}

func (r *reactor) prep(q *request) {
	slot := r.free[len(r.free)-1]
	r.free = r.free[:len(r.free)-1]
	r.slots[slot] = q
	r.inflight++
	s := r.ring.sqe()
	s.opcode, s.fd, s.addr, s.len, s.off, s.opFlags = q.op, q.fd, uint64(uintptr(q.ptr)), q.len, q.off, q.opFlags
	s.userData = uint64(slot)
	q.cqes, q.timedOut = 1, false
	if q.timeout > 0 {
		s.flags = sqeIoLink
		q.ts = syscall.NsecToTimespec(int64(q.timeout))
		t := r.ring.sqe()
		t.opcode, t.fd, t.addr, t.len = opLinkTimeout, -1, uint64(uintptr(unsafe.Pointer(&q.ts))), 1
		t.userData = uint64(slot) | tagTimeout
		q.cqes = 2
	}
	r.outstanding += q.cqes
}

// complete handles one CQE. A request is handed back only after all of its
// CQEs arrived: until then the kernel may still use its buffer and ts.
func (r *reactor) complete(userData uint64, res int32) {
	r.outstanding--
	switch userData {
	case tagWake:
		if !r.halting {
			r.armWake()
		}
		return
	case tagCancel:
		return
	}
	slot := uint32(userData &^ tagTimeout)
	q := r.slots[slot]
	if userData&tagTimeout != 0 {
		q.timedOut = res == -int32(syscall.ETIME)
	} else {
		q.res = res
		r.ops.Add(1)
	}
	if q.cqes--; q.cqes == 0 {
		r.slots[slot] = nil
		r.free = append(r.free, slot)
		r.inflight--
		q.done <- struct{}{}
	}
}

// armWake keeps one read of the eventfd in flight, so a blocked
// io_uring_enter returns when a submitter writes to it.
func (r *reactor) armWake() {
	s := r.ring.sqe()
	s.opcode, s.fd, s.addr, s.len = opRead, int32(r.efd), uint64(uintptr(unsafe.Pointer(&r.wakeBuf))), 8
	s.userData = tagWake
	r.outstanding++
}

// submit queues q and waits for its result. The only syscall a submitter
// ever makes is the eventfd write, and only when the reactor may be asleep.
func (r *reactor) submit(q *request) error {
	r.mu.Lock()
	if r.stopping {
		r.mu.Unlock()
		return net.ErrClosed
	}
	r.queue = append(r.queue, q)
	wake := r.asleep
	r.asleep = false
	r.mu.Unlock()
	if wake {
		r.wake()
	}
	<-q.done
	return nil
}

func (r *reactor) wake() {
	one := [8]byte{1} // adds 1 to the eventfd counter (little endian on amd64 and arm64)
	r.efdMu.RLock()
	if r.efd >= 0 {
		syscall.Write(r.efd, one[:])
	}
	r.efdMu.RUnlock()
}

// stop cancels everything in flight, fails everything queued, and waits for
// the reactor to exit.
func (r *reactor) stop() {
	r.mu.Lock()
	first := !r.stopping
	r.stopping = true
	r.asleep = false
	r.mu.Unlock()
	if first {
		r.wake()
	}
	<-r.exited
}

// Net opens sockets: through io_uring reactors, or through Go's net package
// when it is nil (-io std).
type Net struct {
	rs   []*reactor
	next atomic.Uint32
}

// NewUringNet starts rings reactors. It fails if io_uring is unusable here.
func NewUringNet(rings int) (*Net, error) {
	n := &Net{}
	for range max(rings, 1) {
		r, err := startReactor()
		if err != nil {
			n.Close()
			return nil, err
		}
		n.rs = append(n.rs, r)
	}
	return n, nil
}

func (n *Net) pick() *reactor { return n.rs[(n.next.Add(1)-1)%uint32(len(n.rs))] }

// Close stops the reactors. Sockets still open fail with net.ErrClosed.
func (n *Net) Close() {
	if n == nil {
		return
	}
	for _, r := range n.rs {
		r.stop()
	}
}

// Stats returns socket operations completed and io_uring_enter calls made.
func (n *Net) Stats() (ops, enters uint64) {
	if n == nil {
		return 0, 0
	}
	for _, r := range n.rs {
		ops += r.ops.Load()
		enters += r.enters.Load()
	}
	return ops, enters
}

// slot runs one operation at a time with its reusable request. Holding mu
// for the whole operation also lets Close wait for it.
type slot struct {
	mu sync.Mutex
	q  request
}

// run submits q through r and waits. closed (if not nil) is checked under
// the lock, so no operation can start on an fd that Close already closed
// (the number may belong to a new socket by then).
func (s *slot) run(r *reactor, q request, deadline int64, closed *atomic.Bool) (int32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if closed != nil && closed.Load() {
		return 0, net.ErrClosed
	}
	if deadline != 0 {
		if q.timeout = time.Until(time.Unix(0, deadline)); q.timeout <= 0 {
			return 0, os.ErrDeadlineExceeded
		}
	}
	if q.done = s.q.done; q.done == nil {
		q.done = make(chan struct{}, 1)
	}
	s.q = q
	if err := r.submit(&s.q); err != nil {
		return 0, err
	}
	if s.q.res < 0 && s.q.timedOut {
		return 0, os.ErrDeadlineExceeded
	}
	return s.q.res, nil
}

// closeFd wakes every operation on fd (shutdown, then cancel), waits until
// they are done (each holds its slot's lock), then closes fd.
func closeFd(r *reactor, ctl *slot, fd int32, busy ...*slot) error {
	ctl.run(r, request{op: opShutdown, fd: fd, len: syscall.SHUT_RDWR}, 0, nil)
	ctl.run(r, request{op: opAsyncCancel, fd: fd, opFlags: cancelFd | cancelAll}, 0, nil)
	for _, s := range busy {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	res, err := ctl.run(r, request{op: opClose, fd: fd}, 0, nil)
	if err != nil { // reactor stopped
		return syscall.Close(int(fd))
	}
	if res < 0 {
		return os.NewSyscallError("close", syscall.Errno(-res))
	}
	return nil
}

// uconn is a TCP connection whose every operation goes through a reactor.
type uconn struct {
	r            *reactor
	fd           int32
	rd, wr, ctl  slot
	laddr, raddr net.Addr
	closed       atomic.Bool
	rdDeadline   atomic.Int64 // UnixNano, 0 = none
	wrDeadline   atomic.Int64
}

func (n *Net) newConn(fd int) net.Conn {
	// Go's net sets TCP_NODELAY on every TCP conn; the comparison must be fair.
	syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	la, _ := syscall.Getsockname(fd)
	ra, _ := syscall.Getpeername(fd)
	return &uconn{r: n.pick(), fd: int32(fd), laddr: tcpAddr(la), raddr: tcpAddr(ra)}
}

func (c *uconn) result(op string, res int32, err error) (int, error) {
	switch {
	case c.closed.Load():
		return 0, net.ErrClosed
	case err != nil:
		return 0, err
	case res < 0:
		return 0, os.NewSyscallError(op, syscall.Errno(-res))
	}
	return int(res), nil
}

func (c *uconn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	res, err := c.rd.run(c.r, request{op: opRecv, fd: c.fd, ptr: unsafe.Pointer(&p[0]), len: uint32(min(len(p), maxIO))}, c.rdDeadline.Load(), &c.closed)
	n, err := c.result("recv", res, err)
	if n == 0 && err == nil {
		return 0, io.EOF
	}
	return n, err
}

// Write sends all of p; a single send may take only part of it.
func (c *uconn) Write(p []byte) (int, error) {
	for n := 0; n < len(p); {
		q := request{op: opSend, fd: c.fd, ptr: unsafe.Pointer(&p[n]), len: uint32(min(len(p)-n, maxIO)), opFlags: syscall.MSG_NOSIGNAL}
		res, err := c.wr.run(c.r, q, c.wrDeadline.Load(), &c.closed)
		m, err := c.result("send", res, err)
		if err == nil && m == 0 {
			err = io.ErrShortWrite
		}
		if n += m; err != nil {
			return n, err
		}
	}
	return len(p), nil
}

// CloseWrite half-closes: the peer reads EOF, and we can still read.
func (c *uconn) CloseWrite() error {
	res, err := c.ctl.run(c.r, request{op: opShutdown, fd: c.fd, len: syscall.SHUT_WR}, 0, &c.closed)
	_, err = c.result("shutdown", res, err)
	return err
}

func (c *uconn) Close() error {
	if c.closed.Swap(true) {
		return net.ErrClosed
	}
	return closeFd(c.r, &c.ctl, c.fd, &c.rd, &c.wr)
}

func (c *uconn) LocalAddr() net.Addr  { return c.laddr }
func (c *uconn) RemoteAddr() net.Addr { return c.raddr }

func (c *uconn) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	return c.SetWriteDeadline(t)
}

func (c *uconn) SetReadDeadline(t time.Time) error {
	c.rdDeadline.Store(unixNano(t))
	return nil
}

func (c *uconn) SetWriteDeadline(t time.Time) error {
	c.wrDeadline.Store(unixNano(t))
	return nil
}

func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

type ulistener struct {
	n        *Net
	r        *reactor
	fd       int32
	acc, ctl slot
	addr     net.Addr
	closed   atomic.Bool
}

func (n *Net) Listen(address string) (net.Listener, error) {
	if n == nil {
		return net.Listen("tcp", address)
	}
	ta, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}
	fd, err := listenSocket(ta)
	if err != nil {
		return nil, &net.OpError{Op: "listen", Net: "tcp", Addr: ta, Err: err}
	}
	la, _ := syscall.Getsockname(fd)
	return &ulistener{n: n, r: n.pick(), fd: int32(fd), addr: tcpAddr(la)}, nil
}

// listenSocket: an empty host gets a dual-stack IPv6 socket, like Go's
// net.Listen, or IPv4 where there is no IPv6.
func listenSocket(ta *net.TCPAddr) (int, error) {
	ip := ta.IP
	if ip == nil {
		ip = net.IPv6unspecified
	}
	family, sa, saLen := rawSockaddr(ip, ta.Port)
	fd, err := syscall.Socket(family, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err == syscall.EAFNOSUPPORT && ta.IP == nil {
		family, sa, saLen = rawSockaddr(net.IPv4zero, ta.Port)
		fd, err = syscall.Socket(family, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	}
	if err != nil {
		return -1, os.NewSyscallError("socket", err)
	}
	if family == syscall.AF_INET6 && ta.IP == nil {
		syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, syscall.IPV6_V6ONLY, 0)
	}
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	if _, _, errno := syscall.Syscall(syscall.SYS_BIND, uintptr(fd), uintptr(unsafe.Pointer(sa)), uintptr(saLen)); errno != 0 {
		syscall.Close(fd)
		return -1, os.NewSyscallError("bind", errno)
	}
	if err := syscall.Listen(fd, 4096); err != nil {
		syscall.Close(fd)
		return -1, os.NewSyscallError("listen", err)
	}
	return fd, nil
}

func (l *ulistener) Accept() (net.Conn, error) {
	res, err := l.acc.run(l.r, request{op: opAccept, fd: l.fd, opFlags: syscall.SOCK_CLOEXEC}, 0, &l.closed)
	switch {
	case l.closed.Load():
		if err == nil && res >= 0 {
			syscall.Close(int(res))
		}
		return nil, net.ErrClosed
	case err != nil:
		return nil, err
	case res < 0:
		return nil, os.NewSyscallError("accept", syscall.Errno(-res))
	}
	return l.n.newConn(int(res)), nil
}

func (l *ulistener) Close() error {
	if l.closed.Swap(true) {
		return net.ErrClosed
	}
	return closeFd(l.r, &l.ctl, l.fd, &l.acc)
}

func (l *ulistener) Addr() net.Addr { return l.addr }

// Dial connects with IORING_OP_CONNECT, trying each address of the host in
// turn, all within timeout.
func (n *Net) Dial(address string, timeout time.Duration) (net.Conn, error) {
	if n == nil {
		return net.DialTimeout("tcp", address, timeout)
	}
	deadline := time.Now().Add(timeout)
	ips, port, err := resolve(address, deadline)
	if err != nil {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: err}
	}
	var c net.Conn
	for _, ip := range ips {
		if c, err = n.connect(ip, port, deadline); err == nil || time.Now().After(deadline) {
			break
		}
	}
	if err != nil {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Addr: &net.TCPAddr{IP: ips[0], Port: port}, Err: err}
	}
	return c, nil
}

func (n *Net) connect(ip net.IP, port int, deadline time.Time) (net.Conn, error) {
	family, sa, saLen := rawSockaddr(ip, port)
	fd, err := syscall.Socket(family, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, os.NewSyscallError("socket", err)
	}
	var s slot
	res, err := s.run(n.pick(), request{op: opConnect, fd: int32(fd), ptr: unsafe.Pointer(sa), off: saLen}, deadline.UnixNano(), nil)
	if err == nil && res < 0 {
		err = os.NewSyscallError("connect", syscall.Errno(-res))
	}
	if err != nil {
		syscall.Close(fd)
		return nil, err
	}
	return n.newConn(fd), nil
}

// resolve splits host:port and looks the host up with Go's resolver.
func resolve(address string, deadline time.Time) ([]net.IP, int, error) {
	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return nil, 0, err
	}
	port, err := net.LookupPort("tcp", portStr)
	if err != nil {
		return nil, 0, err
	}
	if host == "" { // like net.Dial: the local system
		host = "localhost"
	}
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, port, nil
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, 0, err
	}
	ips := make([]net.IP, len(addrs))
	for i, a := range addrs {
		ips[i] = a.IP
	}
	return ips, port, nil
}

// rawSockaddr encodes ip:port as the kernel's sockaddr_in or sockaddr_in6.
// It is heap memory, so it stays put while CONNECT or bind reads it.
func rawSockaddr(ip net.IP, port int) (family int, sa *[syscall.SizeofSockaddrInet6]byte, saLen uint64) {
	sa = new([syscall.SizeofSockaddrInet6]byte)
	binary.BigEndian.PutUint16(sa[2:], uint16(port))
	if ip4 := ip.To4(); ip4 != nil {
		binary.NativeEndian.PutUint16(sa[0:], syscall.AF_INET)
		copy(sa[4:8], ip4)
		return syscall.AF_INET, sa, syscall.SizeofSockaddrInet4
	}
	binary.NativeEndian.PutUint16(sa[0:], syscall.AF_INET6)
	copy(sa[8:24], ip.To16()) // after family, port and flowinfo
	return syscall.AF_INET6, sa, syscall.SizeofSockaddrInet6
}

func tcpAddr(sa syscall.Sockaddr) net.Addr {
	switch a := sa.(type) {
	case *syscall.SockaddrInet4:
		return &net.TCPAddr{IP: net.IPv4(a.Addr[0], a.Addr[1], a.Addr[2], a.Addr[3]), Port: a.Port}
	case *syscall.SockaddrInet6:
		return &net.TCPAddr{IP: append(net.IP(nil), a.Addr[:]...), Port: a.Port}
	}
	return &net.TCPAddr{}
}
