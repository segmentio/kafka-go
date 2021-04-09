// Package snappy does nothing, it's kept for backward compatibility to avoid
// breaking the majority of programs that imported it to install the compression
// codec, which is now always included.
package snappy

import "github.com/apoorvag-mav/kafka-go/compress/snappy"

type CompressionCodec = snappy.Codec

type Framing = snappy.Framing

const (
	Code     = 2
	Framed   = snappy.Framed
	Unframed = snappy.Unframed
)

func NewCompressionCodec() *CompressionCodec {
	return NewCompressionCodecFraming(Framed)
}

func NewCompressionCodecFraming(framing Framing) *CompressionCodec {
	return &CompressionCodec{Framing: framing}
}
