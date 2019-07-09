// +build cgo

package zstd

import (
	"io"

	"github.com/DataDog/zstd"
	kafka "github.com/segmentio/kafka-go"
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

const (
	Code int8 = 4
	// https://github.com/DataDog/zstd/blob/1e382f59b41eebd6f592c5db4fd1958ec38a0eba/zstd.go#L33
	DefaultCompressionLevel int = 5
)

func NewCompressionCodec() CompressionCodec {
	return NewCompressionCodecWith(DefaultCompressionLevel)
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
	return zstd.CompressLevel(nil, src, c.CompressionLevel)
}

// Decode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Decode(src []byte) ([]byte, error) {
	return zstd.Decompress(nil, src)
}

// NewReader implements the kafka.CompressionCodec interface.
func (c CompressionCodec) NewReader(r io.Reader) io.ReadCloser {
	return zstd.NewReader(r)
}

// NewWriter implements the kafka.CompressionCodec interface.
func (c CompressionCodec) NewWriter(w io.Writer) io.WriteCloser {
	return zstd.NewWriterLevel(w, c.CompressionLevel)
}
