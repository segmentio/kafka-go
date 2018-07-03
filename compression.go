package kafka

const (
	CompressionNone int8 = iota
	CompressionGZIP
	CompressionSnappy
	CompressionLZ4
)

// CompressionCodec represents a compression codec to encode and decode
// the messages.
type CompressionCodec interface {
	String() string
	Encode(dst, src []byte) (int, error)
	Decode(dst, src []byte) (int, error)
}

const compressionCodecMask int8 = 0x03
const DefaultCompressionLevel int = -1

func init() {
	RegisterCompressionCodec(0, func() CompressionCodec {
		return CompressionCodecNone{}
	})
}

type CompressionCodecNone struct{}

func (c CompressionCodecNone) String() string {
	return "none"
}

func (c CompressionCodecNone) Encode(dst, src []byte) (int, error) {
	return copy(dst, src), nil
}

func (c CompressionCodecNone) Decode(dst, src []byte) (int, error) {
	return copy(dst, src), nil
}

func codecToStr(codec int8) string {
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
