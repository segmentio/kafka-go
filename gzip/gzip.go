package gzip

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"

	"github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(func() kafka.CompressionCodec {
		return NewCompressionCodec()
	})
}

type CompressionCodec struct {
	// CompressionLevel is the level of compression to use on messages.
	CompressionLevel int
}

const Code = 1

func NewCompressionCodec() CompressionCodec {
	return NewCompressionCodecWith(kafka.DefaultCompressionLevel)
}

func NewCompressionCodecWith(level int) CompressionCodec {
	return CompressionCodec{
		CompressionLevel: level,
	}
}

// Code implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Code() int8 {
	return Code
}

// Encode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Encode(src []byte) ([]byte, error) {
	buf := bytes.Buffer{}
	buf.Grow(len(src)) // guess a size to avoid repeat allocations.
	writer := gzip.NewWriter(&buf)

	_, err := writer.Write(src)
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

// Decode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Decode(src []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(reader)
}
