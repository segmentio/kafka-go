package lz4

import (
	"bytes"
	"io/ioutil"

	"github.com/pierrec/lz4"
	"github.com/segmentio/kafka-go"
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
func (c CompressionCodec) Encode(src []byte) ([]byte, error) {
	buf := bytes.Buffer{}
	buf.Grow(len(src)) // guess a size to avoid repeat allocations.

	if c.writer == nil {
		c.writer = lz4.NewWriter(&buf)
	} else {
		c.writer.Reset(&buf)
	}

	_, err := c.writer.Write(src)
	if err != nil {
		return nil, err
	}

	err = c.writer.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), err
}

// Decode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Decode(src []byte) ([]byte, error) {
	if c.reader == nil {
		c.reader = lz4.NewReader(bytes.NewReader(src))
	} else {
		c.reader.Reset(bytes.NewReader(src))
	}
	return ioutil.ReadAll(c.reader)
}
