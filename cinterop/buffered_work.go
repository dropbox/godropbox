package cinterop

import (
	"errors"
	"io"
	"log"
)

// this reads in a loop from socketRead putting batchSize bytes of work to copyTo until
// the socketRead is empty. Will always block until a full workSize of units have been copied
func readBuffer(copyTo chan<- []byte, socketRead io.ReadCloser, batchSize int, workSize int) {
	defer close(copyTo)
	for {
		batch := make([]byte, batchSize)
		size, err := socketRead.Read(batch)
		if err == nil && workSize != 0 && size%workSize != 0 {
			var lsize int
			lsize, err = io.ReadFull(
				socketRead,
				batch[size:size+workSize-(size%workSize)])
			size += lsize
		}
		if size > 0 {
			if err != nil && workSize != 0 {
				size -= (size % workSize)
			}
			copyTo <- batch[:size]
		}
		if err != nil {
			if err != io.EOF {
				log.Print("Error encountered in readBuffer:", err)
			}
			return
		}
	}
}

// this simply copies data from the chan to the socketWrite writer
func writeBuffer(copyFrom <-chan []byte, socketWrite io.Writer) {
	for buf := range copyFrom {
		if len(buf) > 0 {
			size, err := socketWrite.Write(buf)
			if err != nil {
				log.Print("Error encountered in writeBuffer:", err)
				return
			} else if size != len(buf) {
				panic(errors.New("Short Write: io.Writer not compliant"))
			}
		}
	}
}

// this function takes data from socketRead and calls processBatch on a batch of it at a time
// then the resulting bytes are written to wocketWrite as fast as possible
func ProcessBufferedData(
	socketRead io.ReadCloser,
	socketWrite io.Writer,
	// The caller must pass in a factory that returns a pair of functions.
	// The first processes a batch of []bytes and returns the processed bytes
	// The second is called with the same input and the result from the first function after
	// those results have been queued for streaming back to the client,
	// The second function makes it possible to prefetch another batch of data for the client
	makeProcessBatch func() (
		func(input []byte) []byte,
		func(lastInput []byte, lastOutput []byte)),
	batchSize int,
	workItemSize int) {

	readChan := make(chan []byte, 2)
	writeChan := make(chan []byte, 1+batchSize/workItemSize)
	go readBuffer(readChan, socketRead, batchSize, workItemSize)
	go writeBuffer(writeChan, socketWrite)
	pastInit := false
	defer func() { // this is if makeProcessBatch() fails
		if !pastInit {
			if r := recover(); r != nil {
				log.Print("Error in makeProcessBatch ", r)
			}
		}
		socketRead.Close()
		close(writeChan)
	}()
	processBatch, prefetchBatch := makeProcessBatch()
	pastInit = true
	for buf := range readChan {
		result := processBatch(buf)
		writeChan <- result
		prefetchBatch(buf, result)
	}
}
