// +build cgo

package zstd

import (
	"github.com/klauspost/compress/zstd"
	"github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(func() kafka.CompressionCodec {
		return NewCompressionCodec()
	})
}

type CompressionCodec struct {
	// CompressionLevel is the level of compression to use on messages.
	CompressionLevel zstd.EncoderLevel
	encoder          *zstd.Encoder
	decoder          *zstd.Decoder
}

const (
	Code int8 = 4
	// https://github.com/klauspost/compress/blob/315059c39568ee89ae0c118bc6dc7306041543c3/zstd/encoder_options.go#L95
	DefaultCompressionLevel int = 3
)

func NewCompressionCodec() CompressionCodec {
	return NewCompressionCodecWith(DefaultCompressionLevel)
}

func NewCompressionCodecWith(level int) CompressionCodec {
	zStdLevel := zstd.EncoderLevelFromZstd(level)
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zStdLevel))
	dec, _ := zstd.NewReader(nil)

	return CompressionCodec{
		CompressionLevel: zStdLevel,
		encoder:          enc,
		decoder:          dec,
	}
}

// Code implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Code() int8 {
	return Code
}

// Encode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Encode(src []byte) ([]byte, error) {
	return c.encoder.EncodeAll(src, make([]byte, 0, len(src))), nil
}

// Decode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Decode(src []byte) ([]byte, error) {
	return c.decoder.DecodeAll(src, make([]byte, 0, len(src)))
}
