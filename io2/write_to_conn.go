package io2

import (
	"io"
	"io/ioutil"
	"net"
	"sync"
	"sync/atomic"
)

type WriteConn struct {
	isDoneAtomic uint32
	wait         sync.WaitGroup
	conn         ConnWithShutdown
	downstream   io.Writer
	writeErr     error
}

type ConnWithShutdown interface {
	net.Conn
	CloseWrite() error
}

func NewWriteConn(connection ConnWithShutdown, downstream io.Writer) (io.WriteCloser, error) {
	writeConn := WriteConn{
		conn:       connection,
		downstream: downstream,
	}
	writeConn.wait.Add(1)
	go writeConn.copyDownstream()
	return &writeConn, nil
}

func (wcself *WriteConn) copyDownstream() {
	_, wcself.writeErr = io.Copy(wcself.downstream, wcself.conn)
	if wcself.writeErr != nil {
		duplexCloser, ok := wcself.conn.(ConnWithDuplexShutdown)
		if ok {
			_ = duplexCloser.CloseRead()
			_ = duplexCloser.CloseWrite()
		} else {
			_, _ = io.Copy(ioutil.Discard, wcself.conn)
		}
	}
	atomic.AddUint32(&wcself.isDoneAtomic, 1)
	wcself.wait.Done()
}

func (wcself *WriteConn) Write(data []byte) (int, error) {
	if atomic.LoadUint32(&wcself.isDoneAtomic) != 0 {
		wcself.wait.Wait()
		if wcself.writeErr == nil {
			return 0, io.ErrShortWrite
		}
		err := wcself.writeErr
		wcself.writeErr = nil
		return 0, err // return the write error back asap
	}
	return wcself.conn.Write(data)
}

func (wcself *WriteConn) Close() error {
	shutdownErr := wcself.conn.CloseWrite()
	if shutdownErr != nil {
		return shutdownErr
	}
	wcself.wait.Wait()
	socketErr := wcself.conn.Close()
	writeCloser, ok := wcself.downstream.(io.WriteCloser)
	if ok {
		err := writeCloser.Close()
		if (wcself.writeErr != nil &&
			wcself.writeErr != io.EOF) || err != nil {

			if wcself.writeErr != nil {
				err = wcself.writeErr
			}
			return err
		}
	}
	return socketErr
}
