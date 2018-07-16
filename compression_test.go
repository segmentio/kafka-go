package kafka_test

import (
	"math/rand"
	"testing"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/gzip"
	"github.com/segmentio/kafka-go/lz4"
	"github.com/segmentio/kafka-go/snappy"
)

func TestCompression(t *testing.T) {
	msg := kafka.Message{
		Value: []byte("message"),
	}

	testEncodeDecode(t, msg, nil)
	testEncodeDecode(t, msg, gzip.NewCompressionCodec())
	testEncodeDecode(t, msg, snappy.NewCompressionCodec())
	testEncodeDecode(t, msg, lz4.NewCompressionCodec())
}

func testEncodeDecode(t *testing.T, m kafka.Message, codec kafka.CompressionCodec) {
	var r1, r2 kafka.Message
	var err error
	var code int8

	if codec != nil {
		code = codec.Code()
	}

	t.Run("encode with "+codecToStr(code), func(t *testing.T) {
		m.CompressionCodec = codec
		r1, err = m.Encode()
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("encode with "+codecToStr(code), func(t *testing.T) {
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

func codecToStr(codec int8) string {
	switch codec {
	case kafka.CompressionNoneCode:
		return "none"
	case gzip.Code:
		return "gzip"
	case snappy.Code:
		return "snappy"
	case lz4.Code:
		return "lz4"
	default:
		return "unknown"
	}
}

func BenchmarkCompression(b *testing.B) {
	benchmarks := []struct {
		scenario string
		codec    kafka.CompressionCodec
		function func(*testing.B, kafka.CompressionCodec, int, map[int][]byte)
	}{
		{
			scenario: "None",
			codec:    nil,
			function: benchmarkCompression,
		},
		{
			scenario: "GZIP",
			codec:    gzip.NewCompressionCodec(),
			function: benchmarkCompression,
		},
		{
			scenario: "Snappy",
			codec:    snappy.NewCompressionCodec(),
			function: benchmarkCompression,
		},
		{
			scenario: "LZ4",
			codec:    lz4.NewCompressionCodec(),
			function: benchmarkCompression,
		},
	}

	payload := map[int][]byte{
		1024:  randomPayload(1024),
		4096:  randomPayload(4096),
		8192:  randomPayload(8192),
		16384: randomPayload(16384),
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.scenario+"1024", func(b *testing.B) {
			benchmark.function(b, benchmark.codec, 1024, payload)
		})
		b.Run(benchmark.scenario+"4096", func(b *testing.B) {
			benchmark.function(b, benchmark.codec, 4096, payload)
		})
		b.Run(benchmark.scenario+"8192", func(b *testing.B) {
			benchmark.function(b, benchmark.codec, 8192, payload)
		})
		b.Run(benchmark.scenario+"16384", func(b *testing.B) {
			benchmark.function(b, benchmark.codec, 16384, payload)
		})
	}

}

func benchmarkCompression(b *testing.B, codec kafka.CompressionCodec, payloadSize int, payload map[int][]byte) {
	msg := kafka.Message{
		Value:            payload[payloadSize],
		CompressionCodec: codec,
	}

	for i := 0; i < b.N; i++ {
		m1, err := msg.Encode()
		if err != nil {
			b.Fatal(err)
		}

		b.SetBytes(int64(len(m1.Value)))

		_, err = m1.Decode()
		if err != nil {
			b.Fatal(err)
		}

	}
}

const dataset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

func randomPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = dataset[rand.Intn(len(dataset))]
	}
	return b
}
