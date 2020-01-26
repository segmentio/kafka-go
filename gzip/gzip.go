package gzip

import (
	kafka "github.com/segmentio/kafka-go"
	. "github.com/segmentio/kafka-go/protocol/compress/gzip"
)

func init() {
	kafka.RegisterCompressionCodec(NewCompressionCodec())
}
