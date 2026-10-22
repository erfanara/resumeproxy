//go:build linux && (amd64 || arm64)

package main

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

// dialUring connects to addr and hands the connection to io_uring.
func dialUring(t *testing.T, addr string) net.Conn {
	skipNoUring(t)
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c, err := newUringConn(nc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// Reads and writes run concurrently, each on its own ring; roundTrip also
// sets a read deadline, so every read carries a linked timeout.
func TestUringConnRoundTrip(t *testing.T) {
	c := dialUring(t, startEcho(t))
	roundTrip(t, c, randBytes(8<<20), 0)
}

func TestUringConnDeadline(t *testing.T) {
	c := dialUring(t, startEcho(t)) // echo stays silent until we write
	c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	start := time.Now()
	if _, err := c.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("deadline fired after %v", d)
	}
	// Both CQEs of the timed-out read were reaped: the ring still works.
	c.SetReadDeadline(time.Time{})
	roundTrip(t, c, []byte("after timeout"), 0)
}

func TestUringConnCloseUnblocksRead(t *testing.T) {
	c := dialUring(t, startEcho(t))
	done := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 1))
		done <- err
	}()
	time.Sleep(50 * time.Millisecond) // let the read reach the kernel
	c.Close()
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("pending read: want net.ErrClosed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not unblock a pending Read")
	}
	if _, err := c.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("write after close: want net.ErrClosed, got %v", err)
	}
}
