package gzip

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"

	kafka "github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(1, func() kafka.CompressionCodec {
		return CompressionCodec{
			CompressionLevel: kafka.DefaultCompressionLevel,
		}
	})
}

type CompressionCodec struct {
	CompressionLevel int
}

func NewCompressionCodec(level int) CompressionCodec {
	return CompressionCodec{
		CompressionLevel: level,
	}
}

// String implements the kafka.CompressionCodec interface.
func (c CompressionCodec) String() string {
	return "gzip"
}

// Encode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Encode(dst, src []byte) (int, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
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
	reader, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return 0, err
	}
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
