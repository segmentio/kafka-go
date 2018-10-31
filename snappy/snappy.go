package snappy

import (
	"bytes"
	"encoding/binary"

	"github.com/golang/snappy"
	"github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(func() kafka.CompressionCodec {
		return NewCompressionCodec()
	})
}

type CompressionCodec struct{}

const Code = 2

func NewCompressionCodec() CompressionCodec {
	return CompressionCodec{}
}

// Code implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Code() int8 {
	return Code
}

// Encode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Encode(src []byte) ([]byte, error) {
	// NOTE : passing a nil dst means snappy will allocate it.
	return snappy.Encode(nil, src), nil
}

// Decode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Decode(src []byte) ([]byte, error) {
	return decode(src)
}

var xerialHeader = []byte{130, 83, 78, 65, 80, 80, 89, 0}

// From github.com/eapache/go-xerial-snappy
func decode(src []byte) ([]byte, error) {
	if !bytes.Equal(src[:8], xerialHeader) {
		return snappy.Decode(nil, src)
	}

	var (
		pos   = uint32(16)
		max   = uint32(len(src))
		dst   = make([]byte, 0, len(src))
		chunk []byte
		err   error
	)
	for pos < max {
		size := binary.BigEndian.Uint32(src[pos : pos+4])
		pos += 4

		chunk, err = snappy.Decode(chunk, src[pos:pos+size])
		if err != nil {
			return nil, err
		}
		pos += size
		dst = append(dst, chunk...)
	}
	return dst, nil
}
