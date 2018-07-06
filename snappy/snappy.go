package snappy

import (
	"bytes"

	"github.com/eapache/go-xerial-snappy"
	kafka "github.com/segmentio/kafka-go"
)

func init() {
	codec := NewCompressionCodec()
	kafka.RegisterCompressionCodec(codec.Code(), func() kafka.CompressionCodec {
		return codec
	})
}

type CompressionCodec struct{}

func NewCompressionCodec() CompressionCodec {
	return CompressionCodec{}
}

// Code implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Code() int8 {
	return 2
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
