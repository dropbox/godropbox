package hash2

import (
	"bytes"
	"crypto/md5"
)

// WARNING: Do NOT Use MD5 in security contexts (defending against
// intentional manipuations of data from untrusted sources);
// use only for checking data integrity against machine errors.
func ComputeMd5Checksum(data []byte) []byte {
	h := md5.New()
	h.Write(data)
	return h.Sum(nil)
}

func ValidateMd5Checksum(data []byte, sum []byte) bool {
	ourSum := ComputeMd5Checksum(data)
	return bytes.Equal(ourSum, sum)
}
