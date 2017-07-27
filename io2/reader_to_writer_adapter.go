package io2

import (
	"bytes"
	"errors"
	"io"
)

// This class adapts an io.Writer into a io.Reader
// It allows anyone to structure a zlib compression as
// a read operation, rather than a write, making it more
// convenient to fit into a pipeline
type ReaderToWriterAdapter struct {
	upstream     io.Reader
	writeBuffer  splitBufferedWriter
	writer       io.Writer
	offset       int
	closedWriter bool
	readBuffer   []byte
	deferredErr  error
}

// This makes a new ReaderToWriterAdapter
// it requires a factory to generate the writer-to-be-wrapped
// The reason is that the writer will require another "downstream" writer
// to be bound with. Each type of writer (eg zlib.NewWriter) may have
// a slightly different way of being bound with a downstream writer
// so here, the user must provide a factory function into this
// constructor and then the constructor will be invoked with an
// implementation-defined downstream writer, which will allow the writer
// to act as a reader from the outside
func NewReaderToWriterAdapter(writerFactory func(io.Writer) (io.Writer, error),
	upstream io.Reader) (io.ReadCloser, error) {
	retval := ReaderToWriterAdapter{
		writer:     nil,
		upstream:   upstream,
		readBuffer: make([]byte, 65536),
	}
	var err error
	retval.writer, err = writerFactory(&retval.writeBuffer)
	return &retval, err
}

func (rwaself *ReaderToWriterAdapter) drain() error {
	var err error
	var readBuffer [65536]byte
	for err == nil {
		_, err = rwaself.upstream.Read(readBuffer[:])
		// drain the reader if downstream has had a problem?
	}
	return err
}

// this has the same usage as a bytes.Buffer
// however, before using the dynamically sized buffer
// it attempts to fit the data into a user-provided buffer
// this can prevent some extra allocations when the user reads
// the data with an appropriately sized buffer and it can save
// copies in any case
type splitBufferedWriter struct {
	userBuffer      []byte
	userBufferCount int
	remainder       bytes.Buffer
}

// writes, preferably to the userBuffer but then optionally to the remainder
func (sbself *splitBufferedWriter) Write(data []byte) (int, error) {
	toCopy := len(sbself.userBuffer) - sbself.userBufferCount
	if toCopy > len(data) {
		toCopy = len(data)
	}
	copy(sbself.userBuffer[sbself.userBufferCount:sbself.userBufferCount+toCopy], data[:toCopy])
	sbself.userBufferCount += toCopy
	if toCopy < len(data) { // we need to overflow to remainder
		count, err := sbself.remainder.Write(data[toCopy:])
		return count + toCopy, err
	}
	return toCopy, nil
}

func (sbself *splitBufferedWriter) GetAmountCopiedToUser() int {
	return sbself.userBufferCount
}

// removes the user buffer from the splitBufferedWriter
// This makes sure that if the user buffer is only somewhat full, no data remains in the remainder
// This preserves the remainder buffer, since that will be consumed later
func (sbself *splitBufferedWriter) RemoveUserBuffer() (amountReturned int, err error) {
	if len(sbself.userBuffer) > sbself.userBufferCount {
		if len(sbself.remainder.Bytes()) != 0 {
			err = errors.New("remainder must be clear if userBuffer isn't full")
			panic(err)
		}
	}
	amountReturned = sbself.userBufferCount
	sbself.userBuffer = nil
	sbself.userBufferCount = 0
	return

}

// installs a user buffer into the splitBufferedWriter, resetting
// the remainder and original buffer
func (sbself *splitBufferedWriter) InstallNewUserBufferAndResetRemainder(
	data []byte) {

	sbself.remainder.Reset()
	sbself.userBuffer = data
	sbself.userBufferCount = 0
}

func (sbself *splitBufferedWriter) Bytes() []byte {
	return sbself.remainder.Bytes()
}

// implements the Read interface by wrapping the Writer with some buffers
func (rwaself *ReaderToWriterAdapter) Read(data []byte) (int, error) {
	lenToCopy := len(rwaself.writeBuffer.Bytes()) - rwaself.offset
	if lenToCopy > 0 {
		// if we have leftover data from a previous call, we can return that only
		if lenToCopy > len(data) {
			lenToCopy = len(data)
		}
		copy(data[:lenToCopy],
			rwaself.writeBuffer.Bytes()[rwaself.offset:rwaself.offset+lenToCopy])

		rwaself.offset += lenToCopy

		// only return deferred errors if we have consumed the entire remainder
		if rwaself.offset < len(rwaself.writeBuffer.Bytes()) {
			return lenToCopy, nil // if still remainder left, return nil
		} else {
			err := rwaself.deferredErr
			rwaself.deferredErr = nil
			return lenToCopy, err
		}
	}
	rwaself.offset = 0
	// if we have no data from previous runs, lets install the buffer and copy to the writer
	rwaself.writeBuffer.InstallNewUserBufferAndResetRemainder(data)
	for {
		// read from the upstream
		readBufferLenValid, err := rwaself.upstream.Read(rwaself.readBuffer)
		var writeErr error
		var closeErr error
		if readBufferLenValid > 0 {
			// copy data to the writer
			_, writeErr = rwaself.writer.Write(rwaself.readBuffer[:readBufferLenValid])
		}
		if err == io.EOF && !rwaself.closedWriter {
			rwaself.closedWriter = true
			if writeCloser, ok := rwaself.writer.(io.WriteCloser); ok {
				closeErr = writeCloser.Close()
			}
		}
		if err == nil && (writeErr != nil || closeErr != nil) {
			_ = rwaself.drain() // if there was an error with the writer, drain the upstream
		}
		if (err == nil || err == io.EOF) && writeErr != nil {
			err = writeErr
		}
		if (err == nil || err == io.EOF) && closeErr != nil {
			err = closeErr
		}
		if rwaself.writeBuffer.GetAmountCopiedToUser() > 0 || err != nil {
			if rwaself.offset < len(rwaself.writeBuffer.Bytes()) {
				rwaself.deferredErr = err
				err = nil
			}
			amountCopiedToUser, err2 := rwaself.writeBuffer.RemoveUserBuffer()
			if err == nil && err2 != nil {
				err = err2 // this is an internal assertion/check. it should not trip
			} // possibly change to a panic?
			return amountCopiedToUser, err
		}
	}
}

// interrupt the read by closing all resources
func (rwaself *ReaderToWriterAdapter) Close() error {
	var wCloseErr error
	var rCloseErr error
	if writeCloser, ok := rwaself.writer.(io.WriteCloser); ok {
		if !rwaself.closedWriter {
			wCloseErr = writeCloser.Close()
		}
	}
	if readCloser, ok := rwaself.upstream.(io.ReadCloser); ok {
		rCloseErr = readCloser.Close()
	} else {
		rCloseErr = rwaself.drain()
	}
	if rCloseErr != nil && rCloseErr != io.EOF {
		return rCloseErr
	}
	if wCloseErr != nil && wCloseErr != io.EOF {
		return wCloseErr
	}
	return nil
}
