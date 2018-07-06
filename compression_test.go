package kafka_test

import (
	"testing"

	kafka "github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/gzip"
	_ "github.com/segmentio/kafka-go/lz4"
	_ "github.com/segmentio/kafka-go/snappy"
)

func TestCompression(t *testing.T) {
	msg := kafka.Message{
		Value: []byte("message"),
	}

	testEncodeDecode(t, msg, kafka.CompressionNone)
	testEncodeDecode(t, msg, kafka.CompressionGZIP)
	testEncodeDecode(t, msg, kafka.CompressionSnappy)
	testEncodeDecode(t, msg, kafka.CompressionLZ4)
	testUnknownCodec(t, msg, 42)
}

func testEncodeDecode(t *testing.T, m kafka.Message, codec int8) {
	var r1, r2 kafka.Message
	var err error

	t.Run("encode with "+kafka.Codec(codec), func(t *testing.T) {
		m.CompressionCodec = codec
		r1, err = m.Encode()
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("encode with "+kafka.Codec(codec), func(t *testing.T) {
		r2, err = r1.Decode()
		if err != nil {
			t.Error(err)
		}
		if string(r2.Value) != "message" {
			t.Error("bad message")
			t.Log("got: ", r2.Value)
			t.Log("expected: ", []byte("message"))
		}
	})
}

func testUnknownCodec(t *testing.T, m kafka.Message, codec int8) {
	t.Run("unknown codec", func(t *testing.T) {
		expectedErr := "codec unknown not imported."
		m.CompressionCodec = codec
		_, err := m.Encode()
		if err.Error() != expectedErr {
			t.Error("wrong error")
			t.Log("got: ", err)
			t.Error("expected: ", expectedErr)
		}
	})
}
