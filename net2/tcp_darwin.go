// +build darwin

package net2

import (
	"net"
	"syscall"
	"time"
)

func SetTCPUserTimeout(tcpConn *net.TCPConn, timeout time.Duration) error {
	// MacOS does not support unix.TCP_USER_TIMEOUT parameter
	return nil
}

func ControlWithTCPUserTimeout(rawConn syscall.RawConn, timeout time.Duration) error {
	// MacOS does not support unix.TCP_USER_TIMEOUT parameter
	return nil
}
