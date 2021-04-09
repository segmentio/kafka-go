// Package lz4 does nothing, it's kept for backward compatibility to avoid
// breaking the majority of programs that imported it to install the compression
// codec, which is now always included.
package lz4

import "github.com/apoorvag-mav/kafka-go/compress/lz4"

const (
	Code = 3
)

type CompressionCodec = lz4.Codec

func NewCompressionCodec() *CompressionCodec {
	return &CompressionCodec{}
}
