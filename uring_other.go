//go:build !(linux && (amd64 || arm64))

package main

import (
	"errors"
	"net"
)

var errNoUring = errors.New("io_uring is not supported on this platform")

func newUringConn(nc net.Conn) (net.Conn, error) {
	nc.Close()
	return nil, errNoUring
}

func uringProbe() error { return errNoUring }
