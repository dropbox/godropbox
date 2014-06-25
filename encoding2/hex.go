package encoding2

import (
	"fmt"
)

var hexMap [][]byte

// This hex encodes the binary data and writes the encoded data to the writer.
func HexEncodeToWriter(w BinaryWriter, data []byte) {
	for _, b := range data {
		w.Write(hexMap[b])
	}
}

func init() {
	hexMap = make([][]byte, 256)
	for x := 0; x < 256; x++ {
		hexMap[x] = []byte(fmt.Sprintf("%02x", x))
	}
}
