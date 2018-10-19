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
func (c CompressionCodec) Encode(dst, src []byte) (int, error) {
	buf := buffer{data: dst}

	if c.writer == nil {
		c.writer = gzip.NewWriter(&buf)
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
		var err error
		c.reader, err = gzip.NewReader(bytes.NewReader(src))
		if err != nil {
			return 0, err
		}
	} else {
		if err := c.reader.Reset(bytes.NewReader(src)); err != nil {
			return 0, err
		}
	}

	data, err := ioutil.ReadAll(c.reader)
	if err != nil {
		return 0, err
	}
	buf := buffer{data: dst}
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
