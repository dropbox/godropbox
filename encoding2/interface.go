package encoding2

import (
	"io"
)

// An interface for encoding byte values.
type BinaryWriter interface {
	io.Writer
	io.ByteWriter
}
