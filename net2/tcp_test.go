package net2

import (
	"net"
	"sync"
	"time"

	. "gopkg.in/check.v1"

	. "godropbox/gocheck2"
)

type TcpSuite struct {
}

var _ = Suite(&TcpSuite{})

// validate user timeout through zero window case.
func (s *TcpSuite) TestSetTCPUserTimeoutZeroWindow(c *C) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, IsNil)

	// used to be block before read() call to make zero-window case.
	var readLock sync.Mutex
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				readBuf := make([]byte, 512)
				for {
					readLock.Lock()
					n, err := conn.Read(readBuf)
					readLock.Unlock()
					if n == 0 || err != nil {
						break
					}
				}
				defer conn.Close()
			}()
		}
	}()

	// size of buffer should be larger than socket buffer size to make it possible to
	// have unsent data.
	sentBuf := make([]byte, 10240000)

	testFunc := func(fail bool, tcpUserTimeout time.Duration) time.Duration {
		conn, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
		if err != nil {
			c.Assert(err, IsNil)
		}
		if tcpUserTimeout > 0 {
			err = SetTCPUserTimeout(conn.(*net.TCPConn), tcpUserTimeout)
			c.Assert(err, IsNil)
		}

		defer conn.Close()

		startTime := time.Now()
		_, err = conn.Write(sentBuf)
		if fail {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
		return time.Now().Sub(startTime)
	}

	// 1. validating normal case and test environment.
	elapsed := testFunc(false, 0)
	c.Assert(elapsed, LessThan, time.Second)

	// 2. blocking read loop and check sending hangs until unlocking read loop.
	readLock.Lock()
	go func() {
		<-time.After(800 * time.Millisecond)
		readLock.Unlock()
	}()
	elapsed = testFunc(false, 0)
	c.Assert(elapsed, LessThan, time.Second)

	// 3. validate TCP_USER_TIMEOUT timeout.
	// The test might be flaky because it fully rely that window probe will happen during a second,
	// but it depends from TCP_RTO_MIN value.
	readLock.Lock()
	// even set timeout is 100ms it will require one tcp probe which happens after TCP_RTO_MIN time.
	elapsed = testFunc(true, 100*time.Millisecond)
	c.Assert(elapsed, LessThan, time.Second)
}
