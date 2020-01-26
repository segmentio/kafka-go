// Package lz4 installs the lz4 compression codec on import.
package lz4

import (
	kafka "github.com/segmentio/kafka-go"
	. "github.com/segmentio/kafka-go/protocol/compress/lz4"
)

func init() {
	kafka.RegisterCompressionCodec(NewCompressionCodec())
}
