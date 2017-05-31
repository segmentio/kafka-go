package kafka

import (
	"bufio"
	"time"
)

// Message is a data structure representing kafka messages.
type Message struct {
	Offset int64
	Key    []byte
	Value  []byte
	Time   time.Time
}

type message struct {
	CRC        int32
	MagicByte  int8
	Attributes int8
	Timestamp  int64
	Key        []byte
	Value      []byte
}

func (m message) crc32() int32 {
	sum := uint32(0)
	sum = crc32Int8(sum, m.MagicByte)
	sum = crc32Int8(sum, m.Attributes)
	sum = crc32Int64(sum, m.Timestamp)
	sum = crc32Bytes(sum, m.Key)
	sum = crc32Bytes(sum, m.Value)
	return int32(sum)
}

func (m message) size() int32 {
	size := 4 + 1 + 1 + sizeofBytes(m.Key) + sizeofBytes(m.Value)
	if m.MagicByte != 0 {
		size += 8 // Timestamp
	}
	return size
}

func (m message) writeTo(w *bufio.Writer) {
	writeInt32(w, m.CRC)
	writeInt8(w, m.MagicByte)
	writeInt8(w, m.Attributes)
	if m.MagicByte != 0 {
		writeInt64(w, m.Timestamp)
	}
	writeBytes(w, m.Key)
	writeBytes(w, m.Value)
}

type messageSetItem struct {
	Offset      int64
	MessageSize int32
	Message     message
}

func (m messageSetItem) size() int32 {
	return 8 + 4 + m.Message.size()
}

func (m messageSetItem) writeTo(w *bufio.Writer) {
	writeInt64(w, m.Offset)
	writeInt32(w, m.MessageSize)
	m.Message.writeTo(w)
}

type messageSet []messageSetItem

func (s messageSet) size() (size int32) {
	for _, m := range s {
		size += m.size()
	}
	return
}

func (s messageSet) writeTo(w *bufio.Writer) {
	for _, m := range s {
		m.writeTo(w)
	}
}
