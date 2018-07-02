package kafka

import (
	"bufio"
	"fmt"
	"time"
)

// CompressionCodec represents the compression codec available in Kafka
// See : https://cwiki.apache.org/confluence/display/KAFKA/Compression
var codecs map[int8]CompressionCodec

func RegisterCompressionCodec(code int8, str func() string, encode, decode func(src []byte) ([]byte, error)) error {
	if codecs == nil {
		codecs = make(map[int8]CompressionCodec)
	}

	codecs[code] = CompressionCodec{
		str:    str,
		encode: encode,
		decode: decode,
	}
	return nil
}

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
	CompressionCodec int8

	// Compression level for the codec if supported (only gzip)
	CompressionLevel int
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
	m := message{
		MagicByte:  1,
		Key:        msg.Key,
		Value:      msg.Value,
		Timestamp:  timestamp(msg.Time),
		Attributes: int8(msg.CompressionCodec) & compressionCodecMask,
	}
	m.CRC = m.crc32()
	return m
}

func (msg Message) Encode() (Message, error) {
	codec, ok := codecs[msg.CompressionCodec]
	if !ok {
		return msg, fmt.Errorf("codec %s not imported.", codecToStr(msg.CompressionCodec))
	}

	encodedValue, err := codec.encode(msg.Value)
	if err != nil {
		return msg, err
	}

	msg.Value = encodedValue
	return msg, nil
}

func (msg Message) Decode() (Message, error) {
	c := msg.message().Attributes & compressionCodecMask
	codec, ok := codecs[c]
	if !ok {
		return msg, fmt.Errorf("codec %s not imported.", codecToStr(msg.CompressionCodec))
	}

	decodedValue, err := codec.decode(msg.Value)
	if err != nil {
		return msg, err
	}

	msg.Value = decodedValue
	return msg, nil
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
