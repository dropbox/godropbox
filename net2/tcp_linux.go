// +build linux

package net2

import (
	"net"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"godropbox/errors"
)

// SetTCPUserTimeout sets the TCP user timeout on a connection's socket.
// (copy from grpc/internal/syscall/syscall_linux.go).
func SetTCPUserTimeout(tcpConn *net.TCPConn, timeout time.Duration) error {
	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return errors.Wrapf(err, "error getting raw connection: ")
	}

  err = ControlWithTCPUserTimeout(rawConn, timeout)

	if err != nil {
		return errors.Wrapf(err, "error setting option on socket: ")
	}

	return nil
}

// Sets a TCP user timeout on the syscall.RawConn to the given timeout value.
func ControlWithTCPUserTimeout(rawConn syscall.RawConn, timeout time.Duration) error {
  var err error
	err = rawConn.Control(func(fd uintptr) {
		err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(timeout/time.Millisecond))
	})

  return err
}
