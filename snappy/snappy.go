package snappy

import (
	"github.com/eapache/go-xerial-snappy"
	kafka "github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(2, String, Encode, Decode)
}

func String() string {
	return "snappy"
}

func Encode(src []byte, level int) ([]byte, error) {
	return snappy.Encode(src), nil
}

func Decode(src []byte) ([]byte, error) {
	return snappy.Decode(src)
}
