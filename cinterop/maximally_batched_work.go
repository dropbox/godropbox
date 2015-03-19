package cinterop

import (
	"errors"
	"io"
	"log"
)

// this function looks for an entirely zero'd work item and returns true. This lets the client
// control how often the batches get flushed (and it can end a request with a null byte)
func zeroWorkItem(data []byte, workItemSize int) bool {
	if len(data)%workItemSize != 0 {
		data = data[:len(data)-(len(data)%workItemSize)]
	}
	dataLen := len(data)
	for i := 0; i < dataLen; i += workItemSize {
		j := 0
		for j < workItemSize && data[i+j] == 0 {
			j++
		}
		if j == workItemSize {
			return true
		}
	}
	return false
}

// this reads socketRead to fill up the given buffer unless an error is encountered or
// workSize zeros in a row are discovered, aligned with WorkSize, causing a flush
func readUntilNullWorkSizeBatch(socketRead io.ReadCloser,
	batch []byte, workSize int) (size int, err error) {
	err = nil
	size = 0
	if workSize == 0 {
		size, err = io.ReadFull(socketRead, batch)
	} else {
		lastCheckedForNull := 0
		for err == nil {
			var offset int
			offset, err = socketRead.Read(batch[size:])
			size += offset
			if err == nil && size < len(batch) {
				endCheck := size - (size % workSize)
				if zeroWorkItem(batch[lastCheckedForNull:endCheck], workSize) {
					if size%workSize != 0 {
						rem := workSize - (size % workSize)
						offset, err = io.ReadFull(socketRead, batch[size:size+rem])
						size += offset
					}
					return
				}
				lastCheckedForNull = endCheck // need to check partial work items
			} else {
				return
			}
		}
	}
	return
}

// this reads in a loop from socketRead putting batchSize bytes of work to copyTo until
// the socketRead is empty.
// Batches may be shorter than batchSize if a whole workSize element is all zeros
// If workSize of zero is passed in, then the entire batchSize will be filled up regardless,
// unless socketRead returns an error when Read
func readBatch(copyTo chan<- []byte, socketRead io.ReadCloser, batchSize int, workSize int) {
	defer close(copyTo)
	for {
		batch := make([]byte, batchSize)
		size, err := readUntilNullWorkSizeBatch(socketRead, batch, workSize)
		if size > 0 {
			if err != nil && workSize != 0 {
				size -= (size % workSize)
			}
			copyTo <- batch[:size]
		}
		if err != nil {
			if err != io.EOF {
				log.Print("Error encountered in readBatch:", err)
			}
			return
		}
	}
}

// this simply copies data from the chan to the socketWrite writer
func writeBatch(copyFrom <-chan []byte, socketWrite io.Writer) {
	for buf := range copyFrom {
		if len(buf) > 0 {
			size, err := socketWrite.Write(buf)
			if err != nil {
				log.Print("Error encountered in writeBatch:", err)
				return
			} else if size != len(buf) {
				panic(errors.New("Short Write: io.Writer not compliant"))
			}
		}
	}
}

// this function takes data from socketRead and calls processBatch on a batch of it at a time
// then the resulting bytes are written to wocketWrite as fast as possible
func ProcessBatchedData(socketRead io.ReadCloser, socketWrite io.Writer,
	makeProcessBatch func() (func([]byte) []byte, func([]byte, []byte)),
	batchSize int, workItemSize int) {
	readChan := make(chan []byte, 2)
	writeChan := make(chan []byte, 2)
	go readBatch(readChan, socketRead, batchSize, workItemSize)
	go writeBatch(writeChan, socketWrite)
	processBatch, prefetchBatch := makeProcessBatch()
	for buf := range readChan {
		res := processBatch(buf)
		writeChan <- res
		prefetchBatch(buf, res)
	}
	close(writeChan)
}
