package zstd

import (
	kafka "github.com/segmentio/kafka-go"
	. "github.com/segmentio/kafka-go/protocol/compress/zstd"
)

func init() {
	kafka.RegisterCompressionCodec(NewCompressionCodec())
}
