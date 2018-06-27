package kafka

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/eapache/go-xerial-snappy"
)

const compressionCodecMask int8 = 0x03
const defaultCompressionLevel int = -1

// CompressionCodec represents the compression codec available in Kafka
// See : https://cwiki.apache.org/confluence/display/KAFKA/Compression
type CompressionCodec int8

const (
	CompressionNone CompressionCodec = iota
	CompressionGZIP
	CompressionSnappy
	CompressionLZ4
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

	CompressionCodec CompressionCodec
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

func (msg Message) encode() (Message, error) {
	var err error
	switch msg.CompressionCodec {
	case CompressionNone:
		return msg, nil
	case CompressionGZIP:
		var buf bytes.Buffer
		var writer *gzip.Writer

		if msg.CompressionLevel != defaultCompressionLevel {
			writer, err = gzip.NewWriterLevel(&buf, msg.CompressionLevel)
			if err != nil {
				return msg, err
			}
		} else {
			writer = gzip.NewWriter(&buf)
		}
		if _, err := writer.Write(msg.Value); err != nil {
			return msg, err
		}
		if err := writer.Close(); err != nil {
			return msg, err
		}
		msg.Value = buf.Bytes()
		return msg, nil
	case CompressionSnappy:
		msg.Value = snappy.Encode(msg.Value)
		return msg, nil
	default:
		return msg, fmt.Errorf("compression codec not supported.")
	}
}

func (msg Message) decode() (Message, error) {
	var err error

	codec := msg.message().Attributes & compressionCodecMask
	switch CompressionCodec(codec) {
	case CompressionNone:
		return msg, nil
	case CompressionGZIP:
		reader, err := gzip.NewReader(bytes.NewReader(msg.Value))
		if err != nil {
			return msg, err
		}
		msg.Value, err = ioutil.ReadAll(reader)
		return msg, err
	case CompressionSnappy:
		msg.Value, err = snappy.Decode(msg.Value)
		return msg, err
	default:
		return msg, fmt.Errorf("compression codec not supported.")
	}
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
