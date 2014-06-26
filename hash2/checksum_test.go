package hash2

import (
	"bytes"
	"testing"

	"dropbox/util/testing2"
)

var input = []byte("it just works")
var md5Reference = []byte("qp\xbdZ\x9dSH\xe81\x10\x0fk\x81\xff\xda\xdc")

func TestHash(t *testing.T) {
	h := testing2.H{t}

	ourMd5 := ComputeMd5Checksum(input)
	h.Assert(bytes.Equal(ourMd5, md5Reference), "md5s match")

	h.Assert(ValidateMd5Checksum(input, md5Reference), "md5 validates")
	h.Assert(ValidateMd5Checksum(input, ourMd5), "md5 validates")

	// Fails...
	h.Assert(!ValidateMd5Checksum(ourMd5, input), "reversal doesn't work")

	// No panic on empty
	h.Assert(!ValidateMd5Checksum([]byte(""), []byte("")), "wrong sum is wrong")
}
