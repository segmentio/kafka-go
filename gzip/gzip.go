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
	// CompressionLeven is the level of compression to use on messages.
	CompressionLevel int
	writer           *gzip.Writer
	reader           *gzip.Reader
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

	if c.writer == nil {
		c.writer = gzip.NewWriter(&buf)
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
		var err error
		c.reader, err = gzip.NewReader(bytes.NewReader(src))
		if err != nil {
			return nil, err
		}
	} else {
		if err := c.reader.Reset(bytes.NewReader(src)); err != nil {
			return nil, err
		}
	}

	return ioutil.ReadAll(c.reader)
}
