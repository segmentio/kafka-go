package kafka

import "sync"

var codecs = make(map[int8]CompressionCodec)
var codecsMutex sync.RWMutex

// RegisterCompressionCodec registers a compression codec so it can be used by a Writer.
func RegisterCompressionCodec(codec func() CompressionCodec) {
	c := codec()
	codecsMutex.Lock()
	codecs[c.Code()] = c
	codecsMutex.Unlock()
}

// CompressionCodec represents a compression codec to encode and decode
// the messages.
// See : https://cwiki.apache.org/confluence/display/KAFKA/Compression
type CompressionCodec interface {
	// Code returns the compression codec code
	Code() int8

	// Encode encodes the src data and writes the result to dst.
	// If ths destination buffer is too small, the function should
	// return the bytes.ErrToolarge error.
	Encode(dst, src []byte) (int, error)

	// Decode decodes the src data and writes the result to dst.
	// If ths destination buffer is too small, the function should
	// return the bytes.ErrToolarge error.
	Decode(dst, src []byte) (int, error)
}

const compressionCodecMask int8 = 0x03
const DefaultCompressionLevel int = -1
const CompressionNoneCode = 0
