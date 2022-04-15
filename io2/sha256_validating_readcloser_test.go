package io2

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"testing"
)

// $ echo -n foobar | sha256sum
var sha256OfFoobar, _ = hex.DecodeString("c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2")

func checkRead(t *testing.T, r io.Reader, readSize int64, expectedBytes []byte, expectError error) {
	buf := make([]byte, readSize)
	n, err := r.Read(buf)
	t.Logf("Read(%d bytes) = (n = %d, bytes = %v, err = %v)", readSize, n, buf[:n], err)

	if expectError != nil {
		if err != expectError {
			t.Fatalf("Mismatch in expected error during read of size %d - expected %v, actual %v", readSize, expectError, err)
		}
	} else {
		if err != nil {
			t.Fatalf("Unexpected error during Read: %v", err)
		}
	}

	if n != len(expectedBytes) {
		t.Fatalf("Expected to read %d bytes, but got %d bytes", len(expectedBytes), n)
	}
	if !bytes.Equal(expectedBytes, buf[:n]) {
		t.Fatalf("Mismatch in bytes read: Expected '%v', Got '%v'", expectedBytes, buf)
	}
}

func TestValidatePass(t *testing.T) {
	s := NewSHA256ValidatingReader(ioutil.NopCloser(bytes.NewBufferString("foobar")), 6, sha256OfFoobar)
	checkRead(t, s, 3, []byte("foo"), nil)
	checkRead(t, s, 5, []byte("bar"), io.EOF)
	// Subsequent Read's will all return EOF
	checkRead(t, s, 2, nil, io.EOF)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestValidatePrefixOnlyPass(t *testing.T) {
	// Prefix of "foobart" ("foobar") matches the checksum. So, we only end up reading the first 6 bytes
	// from the underlying reader, and then return EOF.
	s := NewSHA256ValidatingReader(ioutil.NopCloser(bytes.NewBufferString("foobart")), 6, sha256OfFoobar)
	checkRead(t, s, 4, []byte("foob"), nil)
	checkRead(t, s, 3, []byte("ar"), io.EOF)
	checkRead(t, s, 2, nil, io.EOF)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateMismatch(t *testing.T) {
	// First n bytes of underlying reader does not have the right checksum.
	s := NewSHA256ValidatingReader(ioutil.NopCloser(bytes.NewBufferString("foobar")), 5, sha256OfFoobar)
	checkRead(t, s, 3, []byte("foo"), nil)
	checkRead(t, s, 3, nil, ErrHashMismatch)
	// Subsequent Read's will all return a non-EOF error.
	checkRead(t, s, 3, nil, ErrHashMismatch)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateShortRead(t *testing.T) {
	// Underlying reader EOF's before returning enough bytes.
	s := NewSHA256ValidatingReader(ioutil.NopCloser(bytes.NewBufferString("foobar")), 7, sha256OfFoobar)
	checkRead(t, s, 6, []byte("foobar"), nil)
	checkRead(t, s, 3, nil, io.ErrUnexpectedEOF)
	// Subsequent Read's will all return a non-EOF error.
	// This check is expecting a specific error, which is stricter than required. But this is
	// acceptable.
	checkRead(t, s, 3, nil, io.ErrUnexpectedEOF)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// Return an error during Read, and subsequently return EOFs
type failOnceReader struct {
	failed bool
}

var failOnceReaderErr error = fmt.Errorf("failOnceReader: Failed read")

func (fr *failOnceReader) Read(b []byte) (int, error) {
	if fr.failed {
		return 0, io.EOF
	}
	fr.failed = true
	return 0, failOnceReaderErr
}

func TestValidateUnderlyingReadFailure(t *testing.T) {
	readCloser := ioutil.NopCloser(io.MultiReader(
		bytes.NewBufferString("fooba"),
		&failOnceReader{},
		bytes.NewBufferString("r"),
	))
	s := NewSHA256ValidatingReader(readCloser, 6, sha256OfFoobar)
	checkRead(t, s, 5, []byte("fooba"), nil)
	// Underlying reader failed. Observe that error.
	checkRead(t, s, 3, nil, failOnceReaderErr)
	// Next read succeeded - all is well.
	checkRead(t, s, 3, []byte("r"), io.EOF)
}
