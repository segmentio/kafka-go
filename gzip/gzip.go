package gzip

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"

	kafka "github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(func() kafka.CompressionCodec {
		return NewCompressionCodec()
	})
}

type CompressionCodec struct {
	CompressionLevel int
	writer           *gzip.Writer
}

func NewCompressionCodec() CompressionCodec {
	return NewCompressionCodecWith(kafka.DefaultCompressionLevel)
}

func NewCompressionCodecWith(level int) CompressionCodec {
	return CompressionCodec{
		CompressionLevel: level,
		writer:           gzip.NewWriter(nil),
	}
}

// Code implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Code() int8 {
	return 1
}

// Encode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Encode(dst, src []byte) (int, error) {
	buf := buffer{
		data: dst,
	}
	c.writer.Reset(&buf)

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
	reader, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return 0, err
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return 0, err
	}
	buf := buffer{
		data: dst,
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
