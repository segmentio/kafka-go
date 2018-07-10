package lz4

import (
	"bytes"

	"github.com/pierrec/lz4"
	kafka "github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(func() kafka.CompressionCodec {
		return NewCompressionCodec()
	})
}

type CompressionCodec struct {
	writer *lz4.Writer
	reader *lz4.Reader
}

const Code = 3

func NewCompressionCodec() CompressionCodec {
	return CompressionCodec{}
}

// Code implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Code() int8 {
	return Code
}

// Encode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Encode(dst, src []byte) (int, error) {
	buf := buffer{data: dst}
	if c.writer == nil {
		c.writer = lz4.NewWriter(&buf)
	} else {
		c.writer.Reset(&buf)
	}

	_, err := c.writer.Write(src)
	if err != nil {
		return 0, err
	}

	err = c.writer.Close()
	if err != nil {
		return 0, err
	}

	return buf.size, err
}

// Decode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Decode(dst, src []byte) (int, error) {
	if c.reader == nil {
		c.reader = lz4.NewReader(bytes.NewReader(src))
	} else {
		c.reader.Reset(bytes.NewReader(src))
	}
	return c.reader.Read(dst)
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
