// +build go1.5

package io2

import (
	"fmt"
	"io"
	"net/http"

	"godropbox/errors"
)

const (
	// prefix string in errors related to source returned by PipelinedCopy() call.
	ErrPrefixSrc = "Failed to read from source: "
	// prefix in errors related to destination.
	ErrPrefixDst = "Failed to write to destination: "
)

// This is similar to io.CopyBuffer, except this uses circular buffer and
// goroutines to pipeline copying (When numBuffers is 1, this simply uses
// io.CopyBuffer).
//
// PipelinedCopy copy will always fill a buffer until it has at least minRead
// bytes in buffer before forwarding the buffer.
// PipelinedCopy flushes dest after each write if it implements http.Flusher
// interface.
//
// PipelinedCopy returns total number of copied bytes, number of allocated
// buffers with bufferSize size each and error.
func PipelinedCopy(
	dest io.Writer,
	src io.Reader,
	numBuffers int,
	bufferSize int,
	minRead int) (written int64, total_buffers int, err error) {

	if numBuffers < 1 {
		return 0, 0, errors.Newf("Invalid number of buffers: %d", numBuffers)
	}

	if bufferSize < 1 {
		return 0, 0, errors.Newf("Invalid buffer size: %d", bufferSize)
	}

	if minRead < 0 {
		return 0, 0, errors.Newf("Invalid min read size: %d", minRead)
	}

	if minRead > bufferSize {
		return 0, 0, errors.Newf(
			"min read size cannot be bigger than buffer size: %d > %d",
			minRead,
			bufferSize)
	}

	if numBuffers == 1 {
		// Nothing to pipeline.  Just use basic copy. (Don't use io.CopyBuffer
		// for streaming requests since it always attempt to fill the entire
		// buffer before forwarding).
		buf := make([]byte, bufferSize, bufferSize)
		w, err := io.CopyBuffer(dest, src, buf)
		return w, 1, err
	}

	copier := newCircularBufferCopier(
		dest,
		src,
		numBuffers,
		bufferSize,
		minRead)
	w, err := copier.execute()
	return w, copier.numCreated, err
}

type buffer struct {
	array []byte // This slice is not resized
	size  int
	eof   bool
}

// A simple ring buffer copier.
type circularBufferCopier struct {
	// The read/write loop will early exit when this channel is closed.
	earlyExitChan chan struct{}

	// Set by read / write loop on exit
	errChan chan error

	readyChan chan *buffer

	recycleChan chan []byte

	// Only used by read loop.
	numBuffers   int
	bufferSize   int
	numCreated   int
	minChunkRead int
	src          io.Reader

	// Only used by write loop.
	numWritten int64
	dest       io.Writer
}

func newCircularBufferCopier(
	dest io.Writer,
	src io.Reader,
	numBuffers int,
	bufferSize int,
	minRead int) *circularBufferCopier {

	if minRead > bufferSize {
		minRead = bufferSize
	}

	// need at least one byte for ReadAtLeast call, otherwise it will not make
	// any Read().
	if minRead == 0 {
		minRead = 1
	}

	return &circularBufferCopier{
		earlyExitChan: make(chan struct{}),
		errChan:       make(chan error, 2),
		readyChan:     make(chan *buffer, numBuffers),
		recycleChan:   make(chan []byte, numBuffers),
		numBuffers:    numBuffers,
		bufferSize:    bufferSize,
		numCreated:    0,
		minChunkRead:  minRead,
		src:           src,
		numWritten:    0,
		dest:          dest,
	}
}

func (c *circularBufferCopier) execute() (int64, error) {
	go c.readLoop()
	go c.writeLoop()

	var err error
	closed := false
	for i := 0; i < 2; i++ {
		e := <-c.errChan
		if e != nil && !closed {
			err = e
			closed = true
			close(c.earlyExitChan)
		}
	}

	return c.numWritten, err
}

func (c *circularBufferCopier) getWriteBuffer() []byte {
	if c.numCreated < c.numBuffers {
		select {
		case b := <-c.recycleChan:
			return b
		case <-c.earlyExitChan:
			return nil
		default:
			c.numCreated++
			return make([]byte, c.bufferSize, c.bufferSize)
		}
	}

	select {
	case b := <-c.recycleChan:
		return b
	case <-c.earlyExitChan:
		return nil
	}
}

func (c *circularBufferCopier) readLoop() {
	for {
		buf := c.getWriteBuffer()
		if buf == nil {
			c.errChan <- nil
			return
		}

		n, err := io.ReadAtLeast(c.src, buf, c.minChunkRead)

		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				c.readyChan <- &buffer{
					array: buf,
					size:  n,
					eof:   true,
				}

				c.errChan <- nil
				return
			}

			c.errChan <- fmt.Errorf("%s%v", ErrPrefixSrc, err)
			return
		}

		c.readyChan <- &buffer{
			array: buf,
			size:  n,
			eof:   false,
		}
	}
}

func (c *circularBufferCopier) writeLoop() {
	for {
		var buf *buffer
		select {
		case buf = <-c.readyChan:
			// do nothing
		case <-c.earlyExitChan:
			c.errChan <- nil
			return
		}

		if buf.size > 0 {
			written, err := c.dest.Write(buf.array[0:buf.size])

			c.numWritten += int64(written)

			if err != nil {
				c.errChan <- fmt.Errorf("%s%v", ErrPrefixDst, err)
				return
			}

			if written != buf.size {
				c.errChan <- errors.New(ErrPrefixDst + "short write")
				return
			}

			if fl, ok := c.dest.(http.Flusher); ok {
				fl.Flush()
			}
		}

		if buf.eof {
			c.errChan <- nil
			return
		}

		c.recycleChan <- buf.array
	}
}
