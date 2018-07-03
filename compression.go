package kafka

const (
	CompressionNone int8 = iota
	CompressionGZIP
	CompressionSnappy
	CompressionLZ4
)

// CompressionCodec represents a compression codec to encode and decode
// the messages.
type CompressionCodec struct {
	str    func() string
	encode func(dst, src []byte) (int, error)
	decode func(dst, src []byte) (int, error)
}

const compressionCodecMask int8 = 0x03
const defaultCompressionLevel int = -1

func init() {
	RegisterCompressionCodec(0,
		func() string { return "none" },
		func(dst, src []byte) (int, error) {
			return copy(dst, src), nil
		},
		func(dst, src []byte) (int, error) {
			return copy(dst, src), nil
		},
	)
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
