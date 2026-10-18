package main

// SOCKS5 front end of the client (RFC 1928 subset: no auth, CONNECT only).
// This is the code that parses untrusted input from local applications.

import (
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"time"
)

func (cl *Client) ServeSocks(ln net.Listener) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go cl.handleSocks(c)
	}
}

// socksReply sends a SOCKS5 reply: VER=5 | REP=code | RSV=0 | ATYP=1 (IPv4)
// | BND.ADDR=0.0.0.0 | BND.PORT=0. Codes used here: 0 ok, 1 general failure,
// 5 connection refused, 7 command not supported, 8 address type not supported.
func socksReply(c net.Conn, code byte) {
	c.Write([]byte{5, code, 0, 1, 0, 0, 0, 0, 0, 0})
}

// handleSocks: SOCKS5, no auth, CONNECT only.
func (cl *Client) handleSocks(c net.Conn) {
	// The handshake must finish quickly; after that the connection may idle forever.
	c.SetDeadline(time.Now().Add(10 * time.Second))
	target, err := socksRequest(c)
	if err != nil {
		c.Close()
		return
	}
	c.SetDeadline(time.Time{})
	// The SOCKS success reply is delayed until the server has actually
	// connected to the target, so the app gets a real "connection refused".
	err = cl.tunnel().Open(target, c, func(ok bool) {
		code := byte(5) // connection refused
		if ok {
			code = 0
		}
		socksReply(c, code)
	})
	if err != nil {
		socksReply(c, 1)
		c.Close()
	}
}

// socksRequest runs the SOCKS5 greeting and reads the CONNECT request (RFC 1928),
// returning the target as "host:port".
func socksRequest(c net.Conn) (string, error) {
	buf := make([]byte, 262) // largest read below: 255-byte domain name plus 2-byte port

	// Greeting: VER=5 | NMETHODS | METHODS[NMETHODS]. We ignore the offered
	// methods and always answer "no authentication" (method 0).
	if _, err := io.ReadFull(c, buf[:2]); err != nil {
		return "", err
	}
	if buf[0] != 5 {
		return "", io.ErrUnexpectedEOF
	}
	if _, err := io.ReadFull(c, buf[:buf[1]]); err != nil {
		return "", err
	}
	if _, err := c.Write([]byte{5, 0}); err != nil {
		return "", err
	}

	// Request: VER | CMD (1 = CONNECT) | RSV | ATYP | DST.ADDR | DST.PORT
	if _, err := io.ReadFull(c, buf[:4]); err != nil {
		return "", err
	}
	if buf[1] != 1 {
		socksReply(c, 7)
		return "", io.ErrUnexpectedEOF
	}
	atyp, n := buf[3], 0
	switch atyp {
	case 1: // IPv4
		n = 4
	case 4: // IPv6
		n = 16
	case 3: // domain name: LEN | NAME. Resolved by the server, so DNS goes through the tunnel too.
		if _, err := io.ReadFull(c, buf[:1]); err != nil {
			return "", err
		}
		n = int(buf[0])
	default:
		socksReply(c, 8)
		return "", io.ErrUnexpectedEOF
	}
	if _, err := io.ReadFull(c, buf[:n+2]); err != nil { // address, then port (big-endian)
		return "", err
	}
	host := string(buf[:n])
	if atyp != 3 {
		host = net.IP(buf[:n]).String()
	}
	return net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(buf[n:])))), nil
}
