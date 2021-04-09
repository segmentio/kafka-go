// Package zstd does nothing, it's kept for backward compatibility to avoid
// breaking the majority of programs that imported it to install the compression
// codec, which is now always included.
package zstd

import "github.com/apoorvag-mav/kafka-go/compress/zstd"

const (
	Code                    = 4
	DefaultCompressionLevel = 3
)

type CompressionCodec = zstd.Codec

func NewCompressionCodec() *CompressionCodec {
	return NewCompressionCodecWith(DefaultCompressionLevel)
}

func NewCompressionCodecWith(level int) *CompressionCodec {
	return &CompressionCodec{Level: level}
}
