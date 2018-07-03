package gzip

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"

	kafka "github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(1, String, Encode, Decode)
}

func String() string {
	return "gzip"
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

func Decode(dst, src []byte) (int, error) {
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
