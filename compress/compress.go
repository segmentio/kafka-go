package compress

import (
	"io"

	"github.com/segmentio/kafka-go/compress/gzip"
	"github.com/segmentio/kafka-go/compress/lz4"
	"github.com/segmentio/kafka-go/compress/snappy"
	"github.com/segmentio/kafka-go/compress/zstd"
)

// Compression represents the the compression applied to a record set.
type Compression int8

const (
	Gzip   Compression = 1
	Snappy Compression = 2
	Lz4    Compression = 3
	Zstd   Compression = 4
)

func (c Compression) Codec() Codec {
	if i := int(c); i >= 0 && i < len(Codecs) {
		return Codecs[i]
	}
	return nil
}

func (c Compression) String() string {
	if codec := c.Codec(); codec != nil {
		return codec.Name()
	}
	return "uncompressed"
}

// Codec represents a compression codec to encode and decode the messages.
// See : https://cwiki.apache.org/confluence/display/KAFKA/Compression
//
// A Codec must be safe for concurrent access by multiple go routines.
type Codec interface {
	// Code returns the compression codec code
	Code() int8

	// Human-readable name for the codec.
	Name() string

	// Constructs a new reader which decompresses data from r.
	NewReader(r io.Reader) io.ReadCloser

	// Constructs a new writer which writes compressed data to w.
	NewWriter(w io.Writer) io.WriteCloser
}

var (
	// The global gzip codec installed on the Codecs table.
	GzipCodec gzip.Codec

	// The global snappy codec installed on the Codecs table.
	SnappyCodec snappy.Codec

	// The global lz4 codec installed on the Codecs table.
	Lz4Codec lz4.Codec

	// The global zstd codec installed on the Codecs table.
	ZstdCodec zstd.Codec

	// The global table of compression codecs supported by the kafka protocol.
	Codecs = [...]Codec{
		Gzip:   &GzipCodec,
		Snappy: &SnappyCodec,
		Lz4:    &Lz4Codec,
		Zstd:   &ZstdCodec,
	}
)
