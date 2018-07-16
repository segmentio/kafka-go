package kafka

import (
	"bufio"
	"bytes"
	"fmt"
	"time"
)

// Message is a data structure representing kafka messages.
type Message struct {
	// Topic is reads only and MUST NOT be set when writing messages
	Topic string

	// Partition is reads only and MUST NOT be set when writing messages
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte

	// If not set at the creation, Time will be automatically set when
	// writing the message.
	Time time.Time

	// Compression codec used to encode the message value
	CompressionCodec
}

func (msg Message) item() messageSetItem {
	item := messageSetItem{
		Offset:  msg.Offset,
		Message: msg.message(),
	}
	item.MessageSize = item.Message.size()
	return item
}

func (msg Message) message() message {
	var attrs int8
	if msg.CompressionCodec != nil {
		attrs = int8(msg.CompressionCodec.Code()) & compressionCodecMask
	}

	m := message{
		MagicByte:  1,
		Key:        msg.Key,
		Value:      msg.Value,
		Timestamp:  timestamp(msg.Time),
		Attributes: attrs,
	}
	m.CRC = m.crc32()
	return m
}

func (msg Message) encode() (Message, error) {
	if msg.CompressionCodec == nil {
		return msg, nil
	}

	var err error
	msg.Value, err = transform(msg.Value, msg.CompressionCodec.Encode)
	return msg, err
}

func (msg Message) decode() (Message, error) {
	c := msg.message().Attributes & compressionCodecMask
	if c == CompressionNoneCode {
		return msg, nil
	}

	codec, ok := codecs[c]
	if !ok {
		return msg, fmt.Errorf("codec %d not imported.", msg.CompressionCodec)
	}

	var err error
	msg.Value, err = transform(msg.Value, codec.Decode)
	return msg, err
}

func transform(value []byte, fn func(dst, src []byte) (int, error)) ([]byte, error) {
	res := make([]byte, len(value))
	n, err := fn(res, value)
	for ; err != nil; n, err = fn(res, value) {
		switch err {
		case bytes.ErrTooLarge:
			res = make([]byte, 2*len(res))
		default:
			return value, err
		}
	}
	return res[:n], nil
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
	return int32(crc32OfMessage(m.MagicByte, m.Attributes, m.Timestamp, m.Key, m.Value))
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
