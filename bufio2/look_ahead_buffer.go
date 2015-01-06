package bufio2

import (
	"io"

	"github.com/dropbox/godropbox/errors"
)

// LookAheadBuffer provides I/O look ahead functionality.  This is particularly
// useful for building parsers.  NOTE: LookAheadBuffer has similar
// functionality as bufio.Reader.  However, unlike bufio.Reader,
// LookAheadBuffer's raw buffer size will EXACTLY match the specified size
// (whereas bufio.Reader's buffer size may differ from the specified size).
// This property ensures the buffer will not accidentally read beyond the
// expected size.
type LookAheadBuffer struct {
	src           io.Reader
	buffer        []byte
	bytesBuffered int
}

// NewLookAheadBuffer returns a new LookAheadBuffer whose raw buffer has EXACTLY
// the specified size.
func NewLookAheadBuffer(src io.Reader, bufferSize int) *LookAheadBuffer {
	return NewLookAheadBufferUsing(src, make([]byte, bufferSize, bufferSize))
}

// NewLookAheadBufferUsing returns a new LookAheadBuffer which uses the
// provided buffer as its raw buffer.  This allows buffer reuse, which reduces
// unnecessary memory allocation.
func NewLookAheadBufferUsing(src io.Reader, rawBuffer []byte) *LookAheadBuffer {
	return &LookAheadBuffer{
		src:           src,
		buffer:        rawBuffer,
		bytesBuffered: 0,
	}
}

// RawBuffer returns the full look ahead buffer.  The raw buffer may not all
// have bytes populated (i.e., b.BytesBuffered() <= len(b.RawBuffer())).
func (b *LookAheadBuffer) RawBuffer() []byte {
	return b.buffer
}

// BytesBuffered returns the number of bytes in the look ahead buffer
// populated by Peek() or PeekAll().
func (b *LookAheadBuffer) BytesBuffered() int {
	return b.bytesBuffered
}

// Buffer returns a slice of the look ahead buffer which holds useful bytes.
// This call is equivalient to b.RawBuffer()[:b.BytesBuffered()].
func (b *LookAheadBuffer) Buffer() []byte {
	return b.buffer[:b.bytesBuffered]
}

// Peek returns a slice of the look ahead buffer which holds numBytes
// number of bytes.  If the look ahead buffer does not already hold enough
// bytes, it will read from the underlying reader to populate the rest.
// NOTE: the returned slice is not a copy of the raw buffer.
func (b *LookAheadBuffer) Peek(numBytes int) ([]byte, error) {
	if numBytes < 0 {
		return nil, errors.New("Cannot fill negative numBytes")
	}
	if numBytes > len(b.buffer) {
		return nil, errors.Newf(
			"Buffer full (buffer size: %d n: %d)",
			len(b.buffer),
			numBytes)
	}

	var err error
	var numRead int
	if b.bytesBuffered < numBytes {
		numRead, err = io.ReadAtLeast(
			b.src,
			b.buffer[b.bytesBuffered:],
			numBytes-b.bytesBuffered)
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				// ErrUnexpectedEOF is a terrible error only returned by
				// ReadAtLeast.  Return EOF (i.e., the original error) instead
				// ErrUnexpectedEOF since no one ever checks for this.
				err = io.EOF
			}
		}
	}

	b.bytesBuffered += numRead

	if numBytes > b.bytesBuffered {
		numBytes = b.bytesBuffered
	}
	return b.buffer[:numBytes], err
}

// PeekAll returns the entire look ahead buffer with all bytes populated.
// If the look ahead buffer does not already hold enough bytes, it will read
// from the underlying reader to populate the rest.  NOTE: the returned slice
// is not a copy of the raw buffer.
func (b *LookAheadBuffer) PeekAll() ([]byte, error) {
	return b.Peek(len(b.buffer))
}

// Consume drops the first numBytes number of populated bytes from the look
// ahead buffer.  NOTE: This is an O(n) operation since it requires shifting
// the remaining bytes to the beginning of the buffer.  Avoid consuming the
// buffer byte by byte.
func (b *LookAheadBuffer) Consume(numBytes int) error {
	if numBytes == 0 {
		return nil
	}
	if numBytes < 0 {
		return errors.New("Cannot drop negative numBytes")
	}
	if b.bytesBuffered < numBytes {
		return errors.New("Consuming more bytes than bytes in buffer.")
	}

	copy(b.buffer, b.buffer[numBytes:b.bytesBuffered])
	b.bytesBuffered -= numBytes
	return nil
}

// ConsumeAll drops all populated bytes from the look ahead buffer.
func (b *LookAheadBuffer) ConsumeAll() {
	err := b.Consume(b.bytesBuffered)
	if err != nil { // This should never happen
		panic(err)
	}
}
