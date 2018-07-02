package kafka_test

import (
	"testing"

	kafka "github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/gzip"
)

func TestCompression(t *testing.T) {
	msg := kafka.Message{
		Value: []byte("message"),
	}

	testEncodeDecode(t, msg, kafka.CompressionNone)
	testEncodeDecode(t, msg, kafka.CompressionGZIP)
}

func testEncodeDecode(t *testing.T, m kafka.Message, codec int8) {
	var r1, r2 kafka.Message
	var err error

	t.Run("encode with "+codecToStr(codec), func(t *testing.T) {
		m.CompressionCodec = codec
		r1, err = m.Encode()
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("encode with "+codecToStr(codec), func(t *testing.T) {
		r2, err = r1.Decode()
		if err != nil {
			t.Error(err)
		}
		if string(r2.Value) != "message" {
			t.Error("bad message")
			t.Log("got: ", string(m.Value))
			t.Log("expected: message")
		}
	})
}

func codecToStr(codec int8) string {
	switch codec {
	case kafka.CompressionNone:
		return "none"
	case kafka.CompressionGZIP:
		return "gzip"
	case kafka.CompressionSnappy:
		return "snappy"
	case kafka.CompressionLZ4:
		return "lz4"
	default:
		return "unknown"
	}
}
