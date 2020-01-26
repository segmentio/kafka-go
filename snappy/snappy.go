package snappy

import (
	kafka "github.com/segmentio/kafka-go"
	. "github.com/segmentio/kafka-go/protocol/compress/snappy"
)

func init() {
	kafka.RegisterCompressionCodec(NewCompressionCodec())
}
