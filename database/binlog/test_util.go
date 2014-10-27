package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// This constructs a raw binlog event and returns its payload.
func CreateEventBytes(
	timestamp uint32,
	eventType uint8,
	serverId uint32,
	nextPosition uint32,
	flags uint16,
	data []byte) ([]byte, error) {

	totalLength := sizeOfBasicV4EventHeader + len(data)

	h := basicV4EventHeader{
		Timestamp:    timestamp,
		EventType:    eventType,
		ServerId:     serverId,
		EventLength:  uint32(totalLength),
		NextPosition: nextPosition,
		Flags:        flags,
	}

	buf := &bytes.Buffer{}

	err := binary.Write(buf, binary.LittleEndian, h)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(data)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func logName(prefix string, num int) string {
	return fmt.Sprintf("%s%06d", prefix, num)
}
