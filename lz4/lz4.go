package lz4

import (
	"bytes"
	"io/ioutil"

	"github.com/pierrec/lz4"
	kafka "github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(3, String, Encode, Decode)
}

func String() string {
	return "lz4"
}

func Encode(src []byte, level int) ([]byte, error) {
	var buf bytes.Buffer
	writer := lz4.NewWriter(&buf)
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
	reader := lz4.NewReader(bytes.NewReader(src))
	return ioutil.ReadAll(reader)
}
