//go:build linux && (amd64 || arm64)

package main

// io_uring backend for the tunnel connection (-io uring), for learning and
// comparison with Go's netpoller. Standard library only.
//
// io_uring is two queues shared between us and the kernel: we put requests
// (SQEs) into the submission queue, io_uring_enter hands them to the kernel
// and waits, and results (CQEs) show up in the completion queue.
//
// The design is the simplest one that works: every connection has one ring
// for reads and one for writes (Session has exactly one reader and one
// writer goroutine), and every Read or Write is one io_uring_enter that
// submits the op and waits for its result. That is still one syscall per
// op, like read(2)/write(2); io_uring only saves syscalls when many ops are
// batched per enter, which this design does not do.

import (
	"errors"
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

// From <linux/io_uring.h>. The syscall numbers are the same on amd64 and
// arm64, hence the build tag.
const (
	sysIoUringSetup = 425
	sysIoUringEnter = 426

	opLinkTimeout = 15
	opSend        = 26
	opRecv        = 27

	sqeIoLink     = 1 << 2 // the next SQE (our timeout) is linked to this one
	enterGetEvent = 1 << 0 // io_uring_enter: wait for completions
	offSQEs       = 0x10000000

	featSingleMmap = 1 << 0 // SQ and CQ rings share one mmap (5.4)
	featRWCurPos   = 1 << 3 // only used as a "kernel is 5.6+" check, like RECV/SEND

	ringSize = 4 // an op and its linked timeout fit with room to spare
)

// uringParams is filled in by io_uring_setup. The offsets say where each
// field of the rings lives inside the mmaped memory.
type uringParams struct {
	sqEntries, cqEntries, flags, sqThreadCPU, sqThreadIdle, features, wqFd uint32
	resv                                                                   [3]uint32
	sqOff                                                                  sqOffsets
	cqOff                                                                  cqOffsets
}

type sqOffsets struct {
	head, tail, ringMask, ringEntries, flags, dropped, array, resv1 uint32
	userAddr                                                        uint64
}

type cqOffsets struct {
	head, tail, ringMask, ringEntries, overflow, cqes, flags, resv1 uint32
	userAddr                                                        uint64
}

type uringSQE struct {
	opcode   uint8
	flags    uint8
	ioprio   uint16
	fd       int32
	off      uint64
	addr     uint64
	len      uint32
	opFlags  uint32 // msg_flags for SEND/RECV
	userData uint64 // copied into the CQE: tells our op and its timeout apart
	_        [24]byte
}

type uringCQE struct {
	userData uint64
	res      int32 // byte count, or -errno
	flags    uint32
}

// ring is one io_uring instance. mu serializes ops; close takes it too, so
// it waits for the op in progress before unmapping the memory.
type ring struct {
	mu      sync.Mutex
	fd      int
	mem     []byte // SQ and CQ rings; nil once closed
	sqeMem  []byte
	sqes    []uringSQE
	sqArray []uint32
	sqHead  *uint32 // written by the kernel as it consumes SQEs
	sqTail  *uint32 // written by us
	cqHead  *uint32 // written by us
	cqTail  *uint32 // written by the kernel as it posts CQEs
	cqes    []uringCQE
	sqMask  uint32
	cqMask  uint32
	ts      syscall.Timespec // the linked timeout reads it during the op
}

func newRing() (*ring, error) {
	var p uringParams
	fd, _, errno := syscall.Syscall(sysIoUringSetup, ringSize, uintptr(unsafe.Pointer(&p)), 0)
	if errno != 0 {
		return nil, os.NewSyscallError("io_uring_setup", errno)
	}
	r := &ring{fd: int(fd)}
	if p.features&(featSingleMmap|featRWCurPos) != featSingleMmap|featRWCurPos {
		r.close()
		return nil, errors.New("io_uring: kernel 5.6 or newer required")
	}
	size := max(p.sqOff.array+4*p.sqEntries, p.cqOff.cqes+uint32(unsafe.Sizeof(uringCQE{}))*p.cqEntries)
	prot, flags := syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE
	var err error
	if r.mem, err = syscall.Mmap(r.fd, 0, int(size), prot, flags); err == nil {
		r.sqeMem, err = syscall.Mmap(r.fd, offSQEs, int(unsafe.Sizeof(uringSQE{}))*int(p.sqEntries), prot, flags)
	}
	if err != nil {
		r.close()
		return nil, os.NewSyscallError("mmap", err)
	}
	// Pointers into the shared memory are taken as &mem[off], never by
	// pointer arithmetic, which keeps -race's checkptr happy.
	u32 := func(off uint32) *uint32 { return (*uint32)(unsafe.Pointer(&r.mem[off])) }
	r.sqHead, r.sqTail, r.sqMask = u32(p.sqOff.head), u32(p.sqOff.tail), *u32(p.sqOff.ringMask)
	r.cqHead, r.cqTail, r.cqMask = u32(p.cqOff.head), u32(p.cqOff.tail), *u32(p.cqOff.ringMask)
	r.sqArray = unsafe.Slice(u32(p.sqOff.array), p.sqEntries)
	r.cqes = unsafe.Slice((*uringCQE)(unsafe.Pointer(&r.mem[p.cqOff.cqes])), p.cqEntries)
	r.sqes = unsafe.Slice((*uringSQE)(unsafe.Pointer(&r.sqeMem[0])), p.sqEntries)
	return r, nil
}

func (r *ring) close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sqeMem != nil {
		syscall.Munmap(r.sqeMem)
	}
	if r.mem != nil {
		syscall.Munmap(r.mem)
	}
	r.mem, r.sqeMem = nil, nil
	if r.fd >= 0 {
		syscall.Close(r.fd)
		r.fd = -1
	}
}

// sqe returns the zeroed SQE in slot i of the submission queue.
func (r *ring) sqe(i uint32) *uringSQE {
	idx := i & r.sqMask
	r.sqArray[idx] = idx
	r.sqes[idx] = uringSQE{}
	return &r.sqes[idx]
}

// do runs one SEND or RECV on fd and returns the kernel's result. With a
// deadline, a LINK_TIMEOUT is linked behind the op and timedOut reports
// whether it fired. do always waits for every CQE it caused, so the kernel
// is done with buf and r.ts when it returns.
func (r *ring) do(fd int, op uint8, buf []byte, msgFlags uint32, deadline time.Time) (res int32, timedOut bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mem == nil {
		return 0, false, net.ErrClosed
	}
	var timeout time.Duration
	if !deadline.IsZero() {
		if timeout = time.Until(deadline); timeout <= 0 {
			return 0, false, os.ErrDeadlineExceeded
		}
	}

	tail := atomic.LoadUint32(r.sqTail)
	s := r.sqe(tail)
	s.opcode, s.fd, s.opFlags, s.userData = op, int32(fd), msgFlags, 1
	// buf escaped to the heap (it came through net.Conn), and Go's heap does
	// not move, so the kernel may keep this address until the CQE arrives.
	s.addr, s.len = uint64(uintptr(unsafe.Pointer(unsafe.SliceData(buf)))), uint32(len(buf))
	n := uint32(1)
	if timeout > 0 {
		s.flags = sqeIoLink
		r.ts = syscall.NsecToTimespec(int64(timeout))
		t := r.sqe(tail + 1)
		t.opcode, t.fd, t.addr, t.len, t.userData = opLinkTimeout, -1, uint64(uintptr(unsafe.Pointer(&r.ts))), 1, 2
		n = 2
	}
	atomic.StoreUint32(r.sqTail, tail+n) // publishes the SQEs to the kernel

	for got := uint32(0); got < n; {
		head := atomic.LoadUint32(r.cqHead)
		if head == atomic.LoadUint32(r.cqTail) {
			// Recomputed every time: Go preempts threads with SIGURG, so
			// enter often returns EINTR after submitting but before our
			// CQEs arrive. Never submit twice, never wait for a posted CQE.
			toSubmit := atomic.LoadUint32(r.sqTail) - atomic.LoadUint32(r.sqHead)
			_, _, errno := syscall.Syscall6(sysIoUringEnter, uintptr(r.fd), uintptr(toSubmit), 1, enterGetEvent, 0, 0)
			switch errno {
			case 0, syscall.EINTR, syscall.EAGAIN, syscall.EBUSY:
				continue
			}
			// Unexpected and fatal for this ring. The caller closes the conn;
			// closing the ring fd makes the kernel cancel what is left.
			return 0, false, os.NewSyscallError("io_uring_enter", errno)
		}
		c := r.cqes[head&r.cqMask]
		if c.userData == 1 {
			res = c.res
		} else {
			timedOut = c.res == -int32(syscall.ETIME)
		}
		atomic.StoreUint32(r.cqHead, head+1)
		got++
	}
	runtime.KeepAlive(buf)
	return res, timedOut, nil
}

// uringConn is a net.Conn whose Read and Write go through io_uring.
type uringConn struct {
	fd           int
	rd, wr       *ring
	laddr, raddr net.Addr
	closed       atomic.Bool
	rdDeadline   atomic.Int64 // UnixNano, 0 = none
	wrDeadline   atomic.Int64
}

// newUringConn takes over the socket of nc, a TCP connection: it keeps a
// duplicate of the fd and closes nc, so Go's netpoller lets go of it. nc is
// closed even on error.
func newUringConn(nc net.Conn) (net.Conn, error) {
	defer nc.Close()
	sc, ok := nc.(syscall.Conn)
	if !ok {
		return nil, errors.New("io_uring: not a socket")
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return nil, err
	}
	fd, dupErr := -1, error(nil)
	if err = raw.Control(func(s uintptr) { fd, dupErr = syscall.Dup(int(s)) }); err == nil {
		err = dupErr
	}
	if err != nil {
		return nil, os.NewSyscallError("dup", err)
	}
	c := &uringConn{fd: fd, laddr: nc.LocalAddr(), raddr: nc.RemoteAddr()}
	// Blocking mode: an op that has to wait for the socket waits in the
	// kernel instead of failing with EAGAIN.
	if err = syscall.SetNonblock(fd, false); err == nil {
		if c.rd, err = newRing(); err == nil {
			c.wr, err = newRing()
		}
	}
	if err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// uringProbe reports whether io_uring is usable here.
func uringProbe() error {
	r, err := newRing()
	r.close()
	return err
}

func (c *uringConn) op(r *ring, op uint8, p []byte, msgFlags uint32, deadline int64) (int, error) {
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	var dl time.Time
	if deadline != 0 {
		dl = time.Unix(0, deadline)
	}
	res, timedOut, err := r.do(c.fd, op, p, msgFlags, dl)
	switch {
	case c.closed.Load(): // also an op that Close woke up
		return 0, net.ErrClosed
	case err != nil:
		return 0, err
	case res >= 0:
		return int(res), nil
	case timedOut: // the op was cancelled by its linked timeout
		return 0, os.ErrDeadlineExceeded
	}
	return 0, os.NewSyscallError("io_uring op", syscall.Errno(-res))
}

func (c *uringConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n, err := c.op(c.rd, opRecv, p, 0, c.rdDeadline.Load())
	if n == 0 && err == nil {
		return 0, io.EOF
	}
	return n, err
}

// Write sends all of p; a single send may take only part of it.
func (c *uringConn) Write(p []byte) (int, error) {
	for n := 0; n < len(p); {
		m, err := c.op(c.wr, opSend, p[n:], syscall.MSG_NOSIGNAL, c.wrDeadline.Load())
		if err == nil && m == 0 {
			err = io.ErrShortWrite
		}
		if n += m; err != nil {
			return n, err
		}
	}
	return len(p), nil
}

// Close wakes a pending Read or Write with shutdown: closing the fd alone
// would not, because an io_uring op holds its own reference to the socket.
// Then it frees the rings (each waits for its op to finish) and the fd.
func (c *uringConn) Close() error {
	if c.closed.Swap(true) {
		return net.ErrClosed
	}
	syscall.Shutdown(c.fd, syscall.SHUT_RDWR)
	c.rd.close()
	c.wr.close()
	return syscall.Close(c.fd)
}

func (c *uringConn) LocalAddr() net.Addr  { return c.laddr }
func (c *uringConn) RemoteAddr() net.Addr { return c.raddr }

func (c *uringConn) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	return c.SetWriteDeadline(t)
}

func (c *uringConn) SetReadDeadline(t time.Time) error {
	c.rdDeadline.Store(unixNano(t))
	return nil
}

func (c *uringConn) SetWriteDeadline(t time.Time) error {
	c.wrDeadline.Store(unixNano(t))
	return nil
}

func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}
