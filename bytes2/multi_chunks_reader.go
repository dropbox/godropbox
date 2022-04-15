package bytes2

import (
	"io"

	"godropbox/errors"
)

type MultiChunksReader struct {
	chunks   [][]byte
	chunkIdx int
	offset   int
}

func NewMultiChunksReader(chunks [][]byte) *MultiChunksReader {
	return &MultiChunksReader{
		chunks:   chunks,
		chunkIdx: 0,
		offset:   0,
	}
}

func (reader *MultiChunksReader) Size() int64 {
	total := 0
	for _, data := range reader.chunks {
		total += len(data)
	}

	return int64(total)
}

func (reader *MultiChunksReader) Read(buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}

	numRead := 0
	for len(buf) > 0 {
		if reader.chunkIdx >= len(reader.chunks) {
			if numRead == 0 {
				return 0, io.EOF
			}

			return numRead, nil
		}

		n := copy(buf, reader.chunks[reader.chunkIdx][reader.offset:])

		buf = buf[n:]
		numRead += n
		reader.offset += n

		if reader.offset >= len(reader.chunks[reader.chunkIdx]) {
			reader.offset = 0
			reader.chunkIdx += 1
		}
	}

	return numRead, nil
}

// NOTE: Seeking behavior EOF is valid.  This matches the behavior of
// bytes.Reader.Seek
func (reader *MultiChunksReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		currPos := int64(reader.offset)
		for i := 0; i < reader.chunkIdx; i++ {
			currPos += int64(len(reader.chunks[i]))
		}
		abs = currPos + offset
	case io.SeekEnd:
		abs = reader.Size() + offset
	default:
		return 0, errors.Newf(
			"MultiChunksReader.Seek: invalid whence %d",
			whence)
	}

	if abs < 0 {
		return 0, errors.New("MultiChunksReader.Seek: negative position")
	}

	remainder := int(abs)

	reader.chunkIdx = 0
	reader.offset = 0
	for reader.chunkIdx < len(reader.chunks) &&
		remainder > len(reader.chunks[reader.chunkIdx]) {

		remainder -= len(reader.chunks[reader.chunkIdx])
		reader.chunkIdx += 1
	}

	reader.offset = remainder

	return abs, nil
}
