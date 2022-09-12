// Package gzip does nothing, it's kept for backward compatibility to avoid
// breaking the majority of programs that imported it to install the compression
// codec, which is now always included.
package gzip

import (
	gz "github.com/klauspost/compress/gzip"
	"github.com/segmentio/kafka-go/compress/gzip"
)

const (
	Code                    = 1
	DefaultCompressionLevel = gz.DefaultCompression
)

type CompressionCodec = gzip.Codec

func NewCompressionCodec() *CompressionCodec {
	return NewCompressionCodecLevel(DefaultCompressionLevel)
}

func NewCompressionCodecLevel(level int) *CompressionCodec {
	return &CompressionCodec{Level: level}
}
