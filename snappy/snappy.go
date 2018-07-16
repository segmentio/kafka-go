package snappy

import (
	"bytes"
	"encoding/binary"

	"github.com/golang/snappy"
	kafka "github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(func() kafka.CompressionCodec {
		return NewCompressionCodec()
	})
}

type CompressionCodec struct {
	writer *snappy.Writer
}

const Code = 2

func NewCompressionCodec() CompressionCodec {
	return CompressionCodec{}
}

// Code implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Code() int8 {
	return Code
}

// Encode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Encode(dst, src []byte) (int, error) {
	if n := snappy.MaxEncodedLen(len(src)); n > len(dst) {
		buf := buffer{data: dst}
		return buf.Write(snappy.Encode(nil, src))
	}
	return len(snappy.Encode(dst, src)), nil
}

// Decode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Decode(dst, src []byte) (int, error) {
	buf := buffer{data: dst}
	data, err := decode(src)
	if err != nil {
		return 0, err
	}
	return buf.Write(data)
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
