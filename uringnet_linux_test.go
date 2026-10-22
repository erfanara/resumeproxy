//go:build linux && (amd64 || arm64)

package main

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

// dialEcho connects to an echo server, both on n.
func dialEcho(t *testing.T, n *Net) net.Conn {
	c, err := n.Dial(startEcho(t, n), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// Listen, Accept, Dial, and reads and writes running at the same time;
// roundTrip also sets a read deadline, so every read has a linked timeout.
func TestUringNetRoundTrip(t *testing.T) {
	roundTrip(t, dialEcho(t, newTestNet(t)), randBytes(8<<20), 0)
}

func TestUringNetDeadline(t *testing.T) {
	c := dialEcho(t, newTestNet(t)) // echo stays silent until we write
	c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	start := time.Now()
	if _, err := c.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("deadline fired after %v", d)
	}
	c.SetReadDeadline(time.Time{})
	roundTrip(t, c, []byte("after timeout"), 0) // the conn still works
}

func TestUringNetCloseUnblocksRead(t *testing.T) {
	c := dialEcho(t, newTestNet(t))
	done := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 1))
		done <- err
	}()
	time.Sleep(50 * time.Millisecond) // let the read reach the kernel
	c.Close()
	waitErr(t, done, net.ErrClosed, "pending Read")
	if _, err := c.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("write after close: want net.ErrClosed, got %v", err)
	}
}

func TestUringNetCloseUnblocksAccept(t *testing.T) {
	ln := listen(t, newTestNet(t))
	done := make(chan error, 1)
	go func() {
		_, err := ln.Accept()
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	ln.Close()
	waitErr(t, done, net.ErrClosed, "pending Accept")
}

func TestUringNetCloseWrite(t *testing.T) {
	n := newTestNet(t)
	ln := listen(t, n)
	accepted := make(chan net.Conn, 1)
	go func() {
		s, _ := ln.Accept()
		accepted <- s
	}()
	c, err := n.Dial(ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s := <-accepted
	defer s.Close()

	c.Write([]byte("hello"))
	if err := c.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(s); err != nil || string(got) != "hello" {
		t.Fatalf("peer read %q, %v; want \"hello\" then EOF", got, err)
	}
	s.Write([]byte("back")) // the other direction still works
	buf := make([]byte, 4)
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "back" {
		t.Fatalf("read %q, %v after CloseWrite", buf, err)
	}
}

func TestUringNetDialRefused(t *testing.T) {
	ln := listen(t, nil)
	addr := ln.Addr().String()
	ln.Close() // nothing listens there now
	if _, err := newTestNet(t).Dial(addr, 2*time.Second); !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("want connection refused, got %v", err)
	}
}

// Many connections busy at once on one reactor: one io_uring_enter must
// carry more than one operation.
func TestUringNetBatches(t *testing.T) {
	skipNoUring(t)
	n, err := NewUringNet(1)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	echo := startEcho(t, n)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := n.Dial(echo, 5*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			roundTrip(t, c, randBytes(1<<20), 0)
		}()
	}
	wg.Wait()
	ops, enters := n.Stats()
	t.Logf("%d ops in %d io_uring_enter calls (%.1f per enter)", ops, enters, float64(ops)/float64(enters))
	if ops <= enters {
		t.Fatalf("no batching: %d ops in %d enters", ops, enters)
	}
}

func waitErr(t *testing.T, done <-chan error, want error, what string) {
	t.Helper()
	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("%s: want %v, got %v", what, want, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Close did not unblock a %s", what)
	}
}
