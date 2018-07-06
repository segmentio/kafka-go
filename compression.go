package kafka

const (
	CompressionNone int8 = iota
	CompressionGZIP
	CompressionSnappy
	CompressionLZ4
)

var codecs map[int8]CompressionCodec

// RegisterCompressionCodec registers a compression codec so it can be used by a Writer.
func RegisterCompressionCodec(codec func() CompressionCodec) {
	if codecs == nil {
		codecs = make(map[int8]CompressionCodec)
	}

	c := codec()
	codecs[c.Code()] = c
}

// CompressionCodec represents a compression codec to encode and decode
// the messages.
// See : https://cwiki.apache.org/confluence/display/KAFKA/Compression
type CompressionCodec interface {
	Code() int8
	Encode(dst, src []byte) (int, error)
	Decode(dst, src []byte) (int, error)
}

const compressionCodecMask int8 = 0x03
const DefaultCompressionLevel int = -1

func init() {
	RegisterCompressionCodec(func() CompressionCodec {
		return CompressionCodecNone{}
	})
}

type CompressionCodecNone struct{}

// Code implements the kafka.CompressionCodec interface.
func (c CompressionCodecNone) Code() int8 {
	return 0
}

// Encode implements the kafka.CompressionCodec interface.
func (c CompressionCodecNone) Encode(dst, src []byte) (int, error) {
	return copy(dst, src), nil
}

// Decode implements the kafka.CompressionCodec interface.
func (c CompressionCodecNone) Decode(dst, src []byte) (int, error) {
	return copy(dst, src), nil
}

// Codec transtales a codec code into the name of the codec.
func Codec(codec int8) string {
	switch codec {
	case CompressionNone:
		return "none"
	case CompressionGZIP:
		return "gzip"
	case CompressionSnappy:
		return "snappy"
	case CompressionLZ4:
		return "lz4"
	default:
		return "unknown"
	}
}
