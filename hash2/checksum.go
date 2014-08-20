package hash2

import (
	"bytes"
	"crypto/md5"
)

// This returns the data's MD5 checksum.
//
// WARNING: Do NOT Use MD5 in security contexts (defending against
// intentional manipulations of data from untrusted sources);
// use only for checking data integrity against machine errors.
func ComputeMd5Checksum(data []byte) []byte {
	h := md5.New()
	h.Write(data)
	return h.Sum(nil)
}

// This returns true iff the data matches the provided checksum.
func ValidateMd5Checksum(data []byte, sum []byte) bool {
	ourSum := ComputeMd5Checksum(data)
	return bytes.Equal(ourSum, sum)
}
