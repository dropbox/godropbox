package io2

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
)

var errReadBeyondEOF = fmt.Errorf("error: reading beyond EOF")
var ErrHashMismatch = fmt.Errorf("error: Hash mismatch")

type sha256ValidatingReadCloser struct {
	rc        io.ReadCloser
	bytesLeft int64
	checksum  []byte
	closed    bool
	err       error
	hasher    hash.Hash
}

// NewSHA256ValidatingReader is an io.ReadCloser implementation which will pass through an
// io.ReadCloser if it produces exactly `size` bytes which has a SHA-256 of `checksum`.
// Fewer than n bytes will be passed through in case of any error. Only the first `size` bytes
// produced by the `reader` will be read.
func NewSHA256ValidatingReader(reader io.ReadCloser, size int64, checksum []byte) io.ReadCloser {
	return &sha256ValidatingReadCloser{
		reader,
		size,
		checksum,
		false,
		nil,
		sha256.New(),
	}
}

func (s *sha256ValidatingReadCloser) Read(d []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}

	if s.bytesLeft == 0 {
		// We've read right number of bytes previous and checksum for read bytes matched expected
		// checksum. All good.
		//
		// Corner case: If the sha256ValidatingReadCloser was created with 0 expected bytes, we don't
		// actually validate the checksum.
		return 0, io.EOF
	}

	if s.bytesLeft < 0 {
		return 0, errReadBeyondEOF
	}

	if s.closed {
		return 0, io.ErrUnexpectedEOF
	}

	if int64(len(d)) > s.bytesLeft {
		d = d[0:s.bytesLeft]
	}
	n, err := s.rc.Read(d)

	if n != 0 {
		s.bytesLeft -= int64(n)
		if s.bytesLeft < 0 {
			_ = s.Close()
			return 0, errReadBeyondEOF
		}

		// Will never return an error
		s.hasher.Write(d[:n])

		if s.bytesLeft == 0 {
			sum := s.hasher.Sum(nil)
			if !bytes.Equal(sum, s.checksum) {
				_ = s.Close()
				s.err = ErrHashMismatch
				return 0, s.err
			}

			// If we get here, then we've read the right number of bytes with the correct checksum. We
			// also ignore any error that may have been returned in the underlying call to Read.
			_ = s.Close()
			return n, io.EOF
		}
	}

	if err == io.EOF {
		// We pass through any underlying errors from the reader through, except EOF.
		// This is a special error that signals valid end of stream. Streaming cache contract requires
		// us to not EOF before we have read exactly N bytes (which we haven't).
		_ = s.Close()
		s.err = io.ErrUnexpectedEOF
		return 0, s.err
	}

	// NOTE: We could have seen other errors, but those are not considered fatal.
	// s.err is not set in such cases.

	return n, err
}

func (s *sha256ValidatingReadCloser) Close() error {
	if !s.closed {
		s.closed = true
		return s.rc.Close()
	}

	return nil
}

var _ io.ReadCloser = (*sha256ValidatingReadCloser)(nil)
