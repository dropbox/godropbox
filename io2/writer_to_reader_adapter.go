package io2

import (
	"io"
)

// This class adapts an io.Reader into a io.WriteCloser
// It allows anyone to structure a zlib decompression as
// a write operation, rather than a read, making it more
// convenient to fit into a pipeline
type WriterToReaderAdapter struct {
	workChan     chan *[]byte
	doneWorkChan chan *[]byte
	errChan      chan error
	closed       bool
}

// This is the reader interface that transforms the output of
// the internal io.Reader and lets that be io.Copy'd into the io.Writer output
type privateReaderAdapter struct {
	bufferToBeRead []byte
	workReceipt    *[]byte
	Writer         WriterToReaderAdapter
	closeReceived  bool
}

// This makes a io.Writer from a io.Reader and begins reading and writing data
// The returned class is not thread safe and public methods must be called from a single thread
func NewWriterToReaderAdapter(toBeAdapted func(io.Reader) (io.Reader, error),
	output io.Writer,
	shouldCloseDownstream bool) io.WriteCloser {
	retval := privateReaderAdapter{
		Writer: WriterToReaderAdapter{
			workChan:     make(chan *[]byte),
			doneWorkChan: make(chan *[]byte),
			errChan:      make(chan error, 3),
			closed:       false,
		}}
	go copyDataToOutput(toBeAdapted, &retval, output, shouldCloseDownstream)
	return &retval.Writer
}

// this is the private Read implementation, to be used with io.Copy
// on the read thread
func (rself *privateReaderAdapter) Read(data []byte) (int, error) {
	lenToCopy := len(rself.bufferToBeRead)
	if lenToCopy == 0 {
		rself.workReceipt = <-rself.Writer.workChan
		if rself.workReceipt == nil {
			// no more data to consume
			rself.closeReceived = true
			return 0, io.EOF
		}
		rself.bufferToBeRead = *rself.workReceipt
		lenToCopy = len(rself.bufferToBeRead)
	}
	if lenToCopy > len(data) {
		lenToCopy = len(data)
	}
	copy(data[:lenToCopy], rself.bufferToBeRead[:lenToCopy])
	rself.bufferToBeRead = rself.bufferToBeRead[lenToCopy:]
	if len(rself.bufferToBeRead) == 0 {
		rself.Writer.doneWorkChan <- rself.workReceipt
		rself.workReceipt = nil
	}
	return lenToCopy, nil
}

// this is the public Write interface that presents any data to the
// companion goroutine (copyDataToOutput)
// This function is unbuffered and blocks until the companion goroutine
// consumes the data and returns a receipt.
// This means that there are no extraneous allocations since the receipt is the data
// that was consumed (sent) and the Write can now return
func (wrself *WriterToReaderAdapter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	wrself.workChan <- &data
	var err error
	select {
	case err = <-wrself.errChan:
	default:
	}
	receipt := <-wrself.doneWorkChan
	if receipt != &data {
		panic("Only one thread allowed to use io.Writer")
	}
	return len(data), err
}

func (wrself *WriterToReaderAdapter) getErrors() (err error) {
	for item := range wrself.errChan {
		if err == nil {
			err = item
		}
	}
	return
}

// Close must be called, even if there's an error during Write,
// to clean up all goroutines and resources
// This function shuts down the Writer, which will deliver
// an io.EOF to the reader class. It then blocks until the
// downstream writer has been passed a close and returns any errors
// from the downstream Close (or any pending errors from final reads
// that  were triggered by the io.Reader to be adapted)
func (wrself *WriterToReaderAdapter) Close() error {
	if wrself.closed {
		panic("Double close on WriterToReaderAdapter")
	}
	wrself.workChan <- nil
	close(wrself.workChan)
	wrself.closed = true
	closeErr := wrself.getErrors()
	close(wrself.doneWorkChan) // once they've sent closing err, they won't be touching this
	return closeErr
}

// drain
//
// This is the final function called when the wrapped io.Reader shuts down and
// stops accepting more input.
//
// this is because readers like zlib don't validate the CRC32
// (the last 4 bytes) in the normal codepath and leave the final buffer unconsumed
func (rself *privateReaderAdapter) drain() {
	if rself.closeReceived {
		return // we have already drained
	}

	if len(rself.bufferToBeRead) != 0 {
		if rself.workReceipt == nil {
			panic("Logic error: if there's data to be read, we must still have the receipt")
		}
		rself.Writer.doneWorkChan <- rself.workReceipt
		rself.workReceipt = nil
	} else {
		if rself.workReceipt != nil {
			panic("Logic error: work receipt should be nil if there's no buffer to drain")
		}
	}

	for toDrain := range rself.Writer.workChan {
		if toDrain == nil {
			break
		} else {
			rself.Writer.doneWorkChan <- toDrain
		}
	}
}

// This io.Copy's as much data as possible from the wrapped reader
// to the corresponding writer output.
// When finished it closes the downstream and drains the upstream
// writer. Finally it sends any remaining errors to the errChan and
// closes that channel
func copyDataToOutput(inputFactory func(io.Reader) (io.Reader, error),
	adaptedInput *privateReaderAdapter,
	output io.Writer,
	shouldCloseDownstream bool) {

	input, err := inputFactory(adaptedInput)

	if err != nil {
		adaptedInput.Writer.errChan <- err
	} else {
		_, err = io.Copy(output, input)
		if err != nil {
			adaptedInput.Writer.errChan <- err
		}
	}
	writeCloser, ok := output.(io.WriteCloser)
	if ok && shouldCloseDownstream {
		closeErr := writeCloser.Close()
		if closeErr != nil {
			adaptedInput.Writer.errChan <- closeErr
		}
	}

	// pulls all the data from the writer until EOF is reached
	// this is because readers like zlib don't validate the CRC32
	// (the last 4 bytes) in the normal codepath
	adaptedInput.drain()

	close(adaptedInput.Writer.errChan)
}
