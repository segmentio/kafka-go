package prototest

import (
	"github.com/apoorvag-mav/kafka-go/protocol"
)

// Bytes constructs a Bytes which exposes the content of b.
func Bytes(b []byte) protocol.Bytes {
	return protocol.NewBytes(b)
}

// String constructs a Bytes which exposes the content of s.
func String(s string) protocol.Bytes {
	return protocol.NewBytes([]byte(s))
}
