package prototest

import (
	"bytes"
	"io"
	"strings"

	"github.com/segmentio/kafka-go/protocol"
)

// Bytes constructs a Bytes which exposes the content of b.
func Bytes(b []byte) protocol.Bytes {
	r := &bytesReader{}
	r.Reader.Reset(b)
	return r
}

type bytesReader struct{ bytes.Reader }

func (r *bytesReader) Close() error { return nil }

func (r *bytesReader) Reset() { r.Seek(0, io.SeekStart) }

// String constructs a Bytes which exposes the content of s.
func String(s string) protocol.Bytes {
	r := &stringReader{}
	r.Reader.Reset(s)
	return r
}

type stringReader struct{ strings.Reader }

func (r *stringReader) Close() error { return nil }

func (r *stringReader) Reset() { r.Seek(0, io.SeekStart) }
