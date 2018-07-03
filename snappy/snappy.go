package snappy

import (
	"bytes"

	"github.com/eapache/go-xerial-snappy"
	kafka "github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(2, func() kafka.CompressionCodec {
		return CompressionCodec{}
	})
}

type CompressionCodec struct{}

// String implements the kafka.CompressionCodec interface.
func (c CompressionCodec) String() string {
	return "snappy"
}

// Encode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Encode(dst, src []byte) (int, error) {
	buf := buffer{data: dst}
	return buf.Write(snappy.Encode(src))
}

// Decode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Decode(dst, src []byte) (int, error) {
	buf := buffer{data: dst}
	data, err := snappy.Decode(src)
	if err != nil {
		return 0, err
	}
	return buf.Write(data)
}

type buffer struct {
	data []byte
	size int
}

func (buf *buffer) Write(b []byte) (int, error) {
	n := copy(buf.data[buf.size:], b)
	buf.size += n
	if n != len(b) {
		return n, bytes.ErrTooLarge
	}
	return n, nil
}
