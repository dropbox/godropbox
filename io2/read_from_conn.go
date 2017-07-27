package io2

import (
	"io"
	"sync"
)

type ConnWithDuplexShutdown interface {
	ConnWithShutdown
	CloseRead() error
}

type ReadConn struct {
	wait     sync.WaitGroup
	conn     ConnWithDuplexShutdown
	upstream io.Reader
	readErr  error
	closed   bool
}

func NewReadConn(connection ConnWithDuplexShutdown, upstream io.Reader) (io.ReadCloser, error) {
	writeConn := ReadConn{
		conn:     connection,
		upstream: upstream,
	}
	writeConn.wait.Add(1)
	go writeConn.copyFromUpstream()
	return &writeConn, nil
}

func (wcself *ReadConn) copyFromUpstream() {
	_, wcself.readErr = io.Copy(wcself.conn, wcself.upstream)
	_ = wcself.conn.CloseWrite()
	wcself.wait.Done()
}

func (wcself *ReadConn) Read(data []byte) (int, error) {
	if wcself.closed {
		panic("undefined behavior")
	}
	cnt, err := wcself.conn.Read(data)

	if err != nil {
		_ = wcself.conn.CloseRead()
		wcself.wait.Wait()
		_ = wcself.conn.Close()
		wcself.closed = true
		if wcself.readErr != nil { // if we got a read error from the goroutine
			err = wcself.readErr // that happened earlier in our pipeline and should
			wcself.readErr = nil // take precedence over the newer error
		}
	}
	return cnt, err
}

func (wcself *ReadConn) Close() error {
	var socketErr error
	if !wcself.closed {
		_ = wcself.conn.CloseRead()
		wcself.wait.Wait()
		socketErr = wcself.conn.Close()
	}
	readCloser, ok := wcself.upstream.(io.ReadCloser)
	if ok {
		err := readCloser.Close()
		if (wcself.readErr != nil && wcself.readErr != io.EOF) || err != nil {
			if wcself.readErr != nil {
				err = wcself.readErr
			}
			return err
		}
	} else if wcself.readErr != nil && wcself.readErr != io.EOF {
		return wcself.readErr
	}
	return socketErr
}
