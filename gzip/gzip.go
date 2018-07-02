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

func Encode(src []byte, level int) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	_, err := writer.Write(src)
	if err != nil {
		return nil, err
	}
	err = writer.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(src []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(reader)
}
