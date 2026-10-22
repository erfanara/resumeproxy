//go:build !(linux && (amd64 || arm64))

package main

import (
	"errors"
	"net"
	"time"
)

// Net is only implemented on Linux (uringnet_linux.go); elsewhere it is
// always nil, which means Go's net package.
type Net struct{}

func NewUringNet(int) (*Net, error) {
	return nil, errors.New("io_uring is not supported on this platform")
}

func (n *Net) Listen(address string) (net.Listener, error) { return net.Listen("tcp", address) }

func (n *Net) Dial(address string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", address, timeout)
}

func (n *Net) Close() {}

func (n *Net) Stats() (ops, enters uint64) { return 0, 0 }
