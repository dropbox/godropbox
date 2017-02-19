package dlog

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"
	"time"
)

type notifyingWriter struct {
	wr io.Writer
	ch chan []byte
}

func (nw *notifyingWriter) Write(b []byte) (int, error) {
	nw.ch <- b
	return nw.wr.Write(b)
}

// Test that flush interval works as advertised for small messages.
func TestConsoleBufferFlushInterval(t *testing.T) {
	wrCh := make(chan []byte)
	nw := &notifyingWriter{ioutil.Discard, wrCh}
	tconbuf := bufferedConsoleT{bufferSize: 32 * 1024, maxFlushInterval: time.Second, baseWr: nw}
	testMsg := []byte("small message")
	tconbuf.Write(testMsg)
	writeTime := time.Now()

	select {
	case data := <-wrCh:
		now := time.Now()
		if !bytes.Equal(data, testMsg) {
			t.Errorf("data mismatch - expected %v, got %v", string(testMsg), string(data))
		}
		// Allow a small fudge factor for clocks.
		flushDelay := now.Sub(writeTime)
		if flushDelay < 900*time.Millisecond || flushDelay > 1100*time.Millisecond {
			t.Errorf("flush delay out of bounds - expected ~%v, got %v",
				tconbuf.maxFlushInterval, flushDelay)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("waited too long for buffer flush")
	}
}
