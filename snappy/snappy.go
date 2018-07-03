package snappy

import (
	"bytes"

	"github.com/eapache/go-xerial-snappy"
	kafka "github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(2, String, Encode, Decode)
}

func String() string {
	return "snappy"
}

type buffer struct {
	data []byte
	size int
}

func (buf *buffer) Write(b []byte) (int, error) {
	n := copy(buf.data[buf.size:], b)
	buf.size += n
	if n != len(b) {
		return n, bytes.ErrTooLarge
	}
	return n, nil
}

func Encode(dst, src []byte) (int, error) {
	buf := buffer{data: dst}
	return buf.Write(snappy.Encode(src))
}

func Decode(dst, src []byte) (int, error) {
	buf := buffer{data: dst}
	data, err := snappy.Decode(src)
	if err != nil {
		return 0, err
	}
	return buf.Write(data)
}
