package binlog

import (
	"bytes"
	"godropbox/errors"
	"io"

	"vitess.io/vitess/go/mysql"
)

// Extracts events from the raw MySQL Binlog Stream.
// Each packet in the stream contains 1-byte header where \00 indicates OK,
// followed by a single v4 encoded event.
type rawBinlogStreamReader struct {
	conn   streamConn
	buf    *bytes.Buffer
	reader EventReader
}

// Interface to simplify test mocking.
type streamConn interface {
	ReadPacket() ([]byte, error)
}

func NewRawV4StreamReader(conn streamConn) EventReader {
	var buffer bytes.Buffer
	return &rawBinlogStreamReader{
		conn:   conn,
		buf:    &buffer,
		reader: NewRawV4EventReader(&buffer, "stream-reader"),
	}
}

func (r *rawBinlogStreamReader) nextPacket() ([]byte, error) {
	buf, err := r.conn.ReadPacket()
	if err != nil {
		return nil, err
	}

	// The binlog stream is padded with one byte between events.
	// The byte should be nil, except in the case of errors.
	switch buf[0] {
	case mysql.OKPacket:
		// This is expected.
	case mysql.ErrPacket:
		err := mysql.ParseErrorPacket(buf)
		return nil, err
	case mysql.EOFPacket:
		return nil, errors.Wrap(io.EOF, "binlog EOF packet, can be retried to see new transactions")
	default:
		return nil, errors.Newf("Error, unexpected binlog stream padding byte %d", buf[0])

	}
	return buf[1:], nil
}

func (r *rawBinlogStreamReader) NextEvent() (Event, error) {
	packet, err := r.nextPacket()
	if err != nil {
		return nil, err
	}
	r.buf.Write(packet)

	event, err := r.reader.NextEvent()
	if err != nil {
		return nil, err
	}

	return event, nil
}

func (r *rawBinlogStreamReader) Close() error {
	return r.reader.Close()
}

func (r *rawBinlogStreamReader) peekHeaderBytes(numBytes int) ([]byte, error) {
	return r.reader.peekHeaderBytes(numBytes)
}

func (r *rawBinlogStreamReader) consumeHeaderBytes(numBytes int) error {
	return r.reader.consumeHeaderBytes(numBytes)
}

func (r *rawBinlogStreamReader) nextEventEndPosition() int64 {
	return r.reader.nextEventEndPosition()
}
