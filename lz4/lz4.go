package lz4

import (
	"bytes"
	"io/ioutil"

	"github.com/pierrec/lz4"
	kafka "github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(3, func() kafka.CompressionCodec {
		return CompressionCodec{}
	})
}

type CompressionCodec struct{}

// String implements the kafka.CompressionCodec interface.
func (c CompressionCodec) String() string {
	return "lz4"
}

// Encode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Encode(dst, src []byte) (int, error) {
	var buf bytes.Buffer
	writer := lz4.NewWriter(&buf)
	_, err := writer.Write(src)
	if err != nil {
		return 0, err
	}
	err = writer.Close()
	if err != nil {
		return 0, err
	}

	n, err := buf.WriteTo(&buffer{
		data: dst,
	})

	return int(n), err
}

// Decode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Decode(dst, src []byte) (int, error) {
	reader := lz4.NewReader(bytes.NewReader(src))
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return 0, err
	}
	buf := buffer{
		data: dst,
	}
	return buf.Write(data)
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
