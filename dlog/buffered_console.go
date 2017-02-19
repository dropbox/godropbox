package dlog

// Wrap the console implementation to buffer writes, yet flush in a
// timely, deterministic fashion, either buffering up to n bytes, or
// for up to t milliseconds, whichever comes first.

import (
	"bufio"
	"flag"
	"io"
	"os"
	"sync"
	"time"
)

type bufferedConsoleT struct {
	mu               sync.Mutex
	wr               io.Writer
	bufferSize       int
	maxFlushInterval time.Duration
	baseWr           io.Writer
}

// The default console is assumed to be os.Stderr, but tests can override.
var bufferedConsole = bufferedConsoleT{baseWr: os.Stderr}

func init() {
	// FIXME(msolo) The main dlog/glog code has a untunable file-based buffer - unify?
	flag.IntVar(&bufferedConsole.bufferSize, "dlog.console-buffer-size", 0,
		"Set the size for the console log buffer.")
	flag.DurationVar(&bufferedConsole.maxFlushInterval, "dlog.console-buffer-max-flush-interval",
		0,
		"Set the maximum time between console flushes if console-buffer-size is non-zero. If the buffer size is exceeded, the console may flush more often than this interval.")
}

func (cb *bufferedConsoleT) Flush() error {
	type flusher interface {
		Flush() error
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if fwr, ok := cb.wr.(flusher); ok {
		return fwr.Flush()
	}
	return nil
}

func (cb *bufferedConsoleT) Sync() error {
	type syncer interface {
		Sync() error
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if swr, ok := cb.wr.(syncer); ok {
		return swr.Sync()
	}
	return nil
}

func (cb *bufferedConsoleT) flushDaemon() {
	if cb.maxFlushInterval > 0 {
		// Try to guarantee that we flush at least every maxFlushInterval.
		// This can result in a single extra queued flush if the
		// underlying writer takes longer maxFlushInterval.
		for _ = range time.Tick(cb.maxFlushInterval) {
			_ = cb.Flush() // Ignore error.
		}
	}
}

func (cb *bufferedConsoleT) Write(b []byte) (n int, err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.wr == nil {
		if cb.bufferSize > 0 {
			cb.wr = bufio.NewWriterSize(cb.baseWr, cb.bufferSize)
			go cb.flushDaemon()
		} else {
			// If logging is invoked before flags are parsed, this slower
			// code path must exist since there is no notification that
			// flags are parsed.
			return cb.baseWr.Write(b)
		}
	}
	return cb.wr.Write(b)
}
