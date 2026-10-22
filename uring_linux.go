//go:build linux && (amd64 || arm64)

package main

// The io_uring ring itself: two queues shared between us and the kernel. We
// put requests (SQEs) into the submission queue, io_uring_enter hands them
// to the kernel, and results (CQEs) show up in the completion queue.
// uringnet_linux.go builds the reactors and sockets on top of it.
// Standard library only.

import (
	"errors"
	"os"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// From <linux/io_uring.h>. The syscall numbers are the same on amd64 and
// arm64, hence the build tag.
const (
	sysIoUringSetup = 425
	sysIoUringEnter = 426

	opAccept      = 13
	opAsyncCancel = 14
	opLinkTimeout = 15
	opConnect     = 16
	opClose       = 19
	opRead        = 22
	opSend        = 26
	opRecv        = 27
	opShutdown    = 34

	sqeIoLink     = 1 << 2 // the next SQE (our timeout) is linked to this one
	enterGetEvent = 1 << 0 // io_uring_enter: process and wait for completions
	offSQEs       = 0x10000000

	cancelAll = 1 << 0 // ASYNC_CANCEL: every matching op, not just the first
	cancelFd  = 1 << 1 // ASYNC_CANCEL: match by fd
	cancelAny = 1 << 2 // ASYNC_CANCEL: match every op

	// Only the reactor's own thread ever enters the ring, so the kernel may
	// run completion work just when that thread enters (DEFER_TASKRUN)
	// instead of interrupting it. SUBMIT_ALL: one bad SQE does not stop
	// the rest of the batch. Together they need kernel 6.1.
	setupSubmitAll    = 1 << 7
	setupSingleIssuer = 1 << 12
	setupDeferTaskrun = 1 << 13

	featSingleMmap = 1 << 0 // SQ and CQ rings share one mmap
	featNoDrop     = 1 << 1 // the kernel never drops a CQE
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
	off      uint64 // also addr2 / addrlen
	addr     uint64
	len      uint32
	opFlags  uint32 // msg_flags, accept_flags, cancel_flags, ...
	userData uint64 // copied into the CQE: says which request it belongs to
	_        [24]byte
}

type uringCQE struct {
	userData uint64
	res      int32 // result, or -errno
	flags    uint32
}

// ring is one io_uring instance. It is not safe for concurrent use: each
// ring belongs to one reactor goroutine.
type ring struct {
	fd      int
	mem     []byte // SQ and CQ rings
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
	sqLen   uint32
	pending uint32 // SQEs written since the last submit
}

func newRing(entries, flags uint32) (*ring, error) {
	p := uringParams{flags: flags}
	fd, _, errno := syscall.Syscall(sysIoUringSetup, uintptr(entries), uintptr(unsafe.Pointer(&p)), 0)
	if errno == syscall.EINVAL {
		return nil, errors.New("io_uring: kernel 6.1 or newer required")
	}
	if errno != 0 {
		return nil, os.NewSyscallError("io_uring_setup", errno)
	}
	r := &ring{fd: int(fd)}
	if p.features&(featSingleMmap|featNoDrop) != featSingleMmap|featNoDrop {
		r.close()
		return nil, errors.New("io_uring: kernel 6.1 or newer required")
	}
	size := max(p.sqOff.array+4*p.sqEntries, p.cqOff.cqes+uint32(unsafe.Sizeof(uringCQE{}))*p.cqEntries)
	prot, mflags := syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE
	var err error
	if r.mem, err = syscall.Mmap(r.fd, 0, int(size), prot, mflags); err == nil {
		r.sqeMem, err = syscall.Mmap(r.fd, offSQEs, int(unsafe.Sizeof(uringSQE{}))*int(p.sqEntries), prot, mflags)
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
	r.sqLen = p.sqEntries
	return r, nil
}

func (r *ring) close() {
	if r.sqeMem != nil {
		syscall.Munmap(r.sqeMem)
	}
	if r.mem != nil {
		syscall.Munmap(r.mem)
	}
	syscall.Close(r.fd)
}

// sqe returns the next free SQE, zeroed. The kernel sees it after submit.
// The caller makes sure no more than sqLen are pending.
func (r *ring) sqe() *uringSQE {
	tail := *r.sqTail + r.pending // only we write the tail
	idx := tail & r.sqMask
	r.sqArray[idx] = idx
	r.sqes[idx] = uringSQE{}
	r.pending++
	return &r.sqes[idx]
}

// enter publishes the pending SQEs and calls io_uring_enter once, waiting
// for at least wait completions. The kernel only processes completions
// (DEFER_TASKRUN) inside this call. It returns early on EINTR, which Go's
// scheduler causes often (it preempts threads with SIGURG); the caller just
// loops. Submitted SQEs are counted from the kernel's SQ head, so nothing
// is ever submitted twice.
func (r *ring) enter(wait uint32) error {
	if r.pending > 0 {
		atomic.StoreUint32(r.sqTail, *r.sqTail+r.pending)
		r.pending = 0
	}
	toSubmit := *r.sqTail - atomic.LoadUint32(r.sqHead)
	_, _, errno := syscall.Syscall6(sysIoUringEnter, uintptr(r.fd), uintptr(toSubmit), uintptr(wait), enterGetEvent, 0, 0)
	switch errno {
	case 0, syscall.EINTR, syscall.EAGAIN, syscall.EBUSY:
		return nil
	}
	return os.NewSyscallError("io_uring_enter", errno)
}

// reap calls f for every CQE that has arrived and frees their slots.
func (r *ring) reap(f func(userData uint64, res int32)) int {
	head, tail := *r.cqHead, atomic.LoadUint32(r.cqTail)
	for i := head; i != tail; i++ {
		c := &r.cqes[i&r.cqMask]
		f(c.userData, c.res)
	}
	atomic.StoreUint32(r.cqHead, tail)
	return int(tail - head)
}
