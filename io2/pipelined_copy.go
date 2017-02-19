// +build go1.5

package io2

import (
	"io"
	"net/http"

	"github.com/dropbox/godropbox/errors"
)

const (
	maxFastStartInitialRead = 64
)

// This is similar to io.CopyBuffer, except this uses circular buffer and
// goroutines to pipeline copying (When numBuffers is 1, this simply uses
// io.CopyBuffer).
//
// If fastStart is false, pipelined copy will always fill a buffer until it is
// at least 90% full before forwarding the buffer.  When fastStart is true,
// pipelined copy will fill the initial buffer until it is at least 10% full
// before forwarding the buffer.  The minimum % fill is gradually increased for
// subsequent buffers, capping out at 90%.
func PipelinedCopy(
	dest io.Writer,
	src io.Reader,
	numBuffers int,
	bufferSize int,
	fastStart bool) (written int64, err error) {

	if numBuffers < 1 {
		return 0, errors.Newf("Invalid number of buffers: %d", numBuffers)
	}

	if bufferSize < 1 {
		return 0, errors.Newf("Invalid buffer size: %s", bufferSize)
	}

	if numBuffers == 1 && !fastStart {
		// Nothing to pipeline.  Just use basic copy. (Don't use io.CopyBuffer
		// when fastStart is true since it always attempt to fill the entire
		// buffer before forwarding).
		buf := make([]byte, bufferSize, bufferSize)
		return io.CopyBuffer(dest, src, buf)
	}

	copier := newCircularBufferCopier(
		dest,
		src,
		numBuffers,
		bufferSize,
		fastStart)
	return copier.execute()
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
	fastStart    bool
	minChunkRead int
	src          io.Reader

	// Only used by write loop.
	numWritten int64
	dest       io.Writer
}

func minRead(bufferSize int) int {
	minChunkRead := int(0.9*float64(bufferSize) + 0.5)
	if minChunkRead < bufferSize-1024 {
		minChunkRead = bufferSize - 1024
	}

	return minChunkRead
}

func newCircularBufferCopier(
	dest io.Writer,
	src io.Reader,
	numBuffers int,
	bufferSize int,
	fastStart bool) *circularBufferCopier {

	return &circularBufferCopier{
		earlyExitChan: make(chan struct{}),
		errChan:       make(chan error, 2),
		readyChan:     make(chan *buffer, numBuffers),
		recycleChan:   make(chan []byte, numBuffers),
		numBuffers:    numBuffers,
		bufferSize:    bufferSize,
		numCreated:    0,
		fastStart:     fastStart,
		minChunkRead:  minRead(bufferSize),
		src:           src,
		numWritten:    0,
		dest:          dest,
	}
}

func (c *circularBufferCopier) execute() (int64, error) {
	go c.readLoop()
	go c.writeLoop()

	var err error
	for i := 0; i < 2; i++ {
		e := <-c.errChan
		if e != nil {
			err = e
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
	minChunkRead := c.minChunkRead
	if c.fastStart {
		minChunkRead = int(float64(c.bufferSize) * 0.1)
		if minChunkRead > maxFastStartInitialRead {
			minChunkRead = maxFastStartInitialRead
		}
	}

	for {
		buf := c.getWriteBuffer()
		if buf == nil {
			c.errChan <- nil
			return
		}

		n, err := io.ReadAtLeast(c.src, buf, minChunkRead)

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

			c.errChan <- errors.Wrap(err, "Failed to read from source: ")
			return
		}

		c.readyChan <- &buffer{
			array: buf,
			size:  n,
			eof:   false,
		}

		if c.fastStart {
			minChunkRead *= 2
			if minChunkRead > c.minChunkRead {
				minChunkRead = c.minChunkRead
			}
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
				c.errChan <- errors.Wrap(
					err,
					"Failed to write to destination: ")
				return
			}

			if written != buf.size {
				c.errChan <- errors.New(
					"Failed to write to destination: short write")
				return
			}

			// immediate sending data is more important for fast start instead of
			// buffering, so it makes sense to flush data which calls Write of all
			// sequential buffers.
			if c.fastStart {
				if fl, ok := c.dest.(http.Flusher); ok {
					fl.Flush()
				}
			}
		}

		if buf.eof {
			c.errChan <- nil
			return
		}

		c.recycleChan <- buf.array
	}
}
