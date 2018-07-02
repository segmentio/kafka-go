package kafka

const (
	CompressionNone int8 = iota
	CompressionGZIP
	CompressionSnappy
	CompressionLZ4
)

type CompressionCodec struct {
	str    func() string
	encode func(src []byte) ([]byte, error)
	decode func(src []byte) ([]byte, error)
}

const compressionCodecMask int8 = 0x03
const defaultCompressionLevel int = -1

func init() {
	RegisterCompressionCodec(0,
		func() string { return "none" },
		func(src []byte) ([]byte, error) { return src, nil },
		func(src []byte) ([]byte, error) { return src, nil },
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
