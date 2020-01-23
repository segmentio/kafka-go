package protocol

import (
	"io"

	"github.com/segmentio/kafka-go/protocol/compress/gzip"
	"github.com/segmentio/kafka-go/protocol/compress/lz4"
	"github.com/segmentio/kafka-go/protocol/compress/snappy"
	"github.com/segmentio/kafka-go/protocol/compress/zstd"
)

// Compression represents the the compression applied to a record set.
type Compression int8

func (c Compression) Codec() CompressionCodec {
	if i := int(c); i >= 0 && i < len(compressionCodecs) {
		return compressionCodecs[i]
	}
	return nil
}

// CompressionCodec represents a compression codec to encode and decode
// the messages.
// See : https://cwiki.apache.org/confluence/display/KAFKA/Compression
//
// A CompressionCodec must be safe for concurrent access by multiple go
// routines.
type CompressionCodec interface {
	// Code returns the compression codec code
	Code() int8

	// Human-readable name for the codec.
	Name() string

	// Constructs a new reader which decompresses data from r.
	NewReader(r io.Reader) io.ReadCloser

	// Constructs a new writer which writes compressed data to w.
	NewWriter(w io.Writer) io.WriteCloser
}

var compressionCodecs = [...]CompressionCodec{
	Gzip:   gzip.NewCompressionCodec(),
	Snappy: snappy.NewCompressionCodec(),
	Lz4:    lz4.NewCompressionCodec(),
	Zstd:   zstd.NewCompressionCodec(),
}
