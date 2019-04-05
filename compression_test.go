package kafka_test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/gzip"
	"github.com/segmentio/kafka-go/lz4"
	"github.com/segmentio/kafka-go/snappy"
	ktesting "github.com/segmentio/kafka-go/testing"
	"github.com/segmentio/kafka-go/zstd"
)

func TestCompression(t *testing.T) {
	msg := kafka.Message{
		Value: []byte("message"),
	}

	testEncodeDecode(t, msg, gzip.NewCompressionCodec())
	testEncodeDecode(t, msg, snappy.NewCompressionCodec())
	testEncodeDecode(t, msg, lz4.NewCompressionCodec())
	if ktesting.KafkaIsAtLeast("2.1.0") {
		testEncodeDecode(t, msg, zstd.NewCompressionCodec())
	}
}

func testEncodeDecode(t *testing.T, m kafka.Message, codec kafka.CompressionCodec) {
	var r1, r2 []byte
	var err error
	var code int8

	if codec != nil {
		code = codec.Code()
	}

	t.Run("encode with "+codecToStr(code), func(t *testing.T) {
		r1, err = codec.Encode(m.Value)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("decode with "+codecToStr(code), func(t *testing.T) {
		r2, err = codec.Decode(r1)
		if err != nil {
			t.Error(err)
		}
		if string(r2) != "message" {
			t.Error("bad message")
			t.Log("got: ", string(r2))
			t.Log("expected: ", string(m.Value))
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
	case zstd.Code:
		return "zstd"
	default:
		return "unknown"
	}
}

func TestCompressedMessages(t *testing.T) {
	testCompressedMessages(t, gzip.NewCompressionCodec())
	testCompressedMessages(t, snappy.NewCompressionCodec())
	testCompressedMessages(t, lz4.NewCompressionCodec())

	if ktesting.KafkaIsAtLeast("2.1.0") {
		testCompressedMessages(t, zstd.NewCompressionCodec())
	}
}

func testCompressedMessages(t *testing.T, codec kafka.CompressionCodec) {
	t.Run("produce/consume with"+codecToStr(codec.Code()), func(t *testing.T) {
		t.Parallel()

		topic := kafka.CreateTopic(t, 1)
		w := kafka.NewWriter(kafka.WriterConfig{
			Brokers:          []string{"127.0.0.1:9092"},
			Topic:            topic,
			CompressionCodec: codec,
			BatchTimeout:     10 * time.Millisecond,
		})
		defer w.Close()

		offset := 0
		var values []string
		for i := 0; i < 10; i++ {
			batch := make([]kafka.Message, i+1)
			for j := range batch {
				value := fmt.Sprintf("Hello World %d!", offset)
				values = append(values, value)
				batch[j] = kafka.Message{
					Key:   []byte(strconv.Itoa(offset)),
					Value: []byte(value),
				}
				offset++
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := w.WriteMessages(ctx, batch...); err != nil {
				t.Errorf("error sending batch %d, reason: %+v", i+1, err)
			}
			cancel()
		}

		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{"127.0.0.1:9092"},
			Topic:     topic,
			Partition: 0,
			MaxWait:   10 * time.Millisecond,
			MinBytes:  1,
			MaxBytes:  1024,
		})
		defer r.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// in order to ensure proper handling of decompressing message, read at
		// offsets that we know to be in the middle of compressed message sets.
		for base := range values {
			r.SetOffset(int64(base))
			for i := base; i < len(values); i++ {
				msg, err := r.ReadMessage(ctx)
				if err != nil {
					t.Errorf("error receiving message at loop %d, offset %d, reason: %+v", base, i, err)
				}
				if msg.Offset != int64(i) {
					t.Errorf("wrong offset at loop %d...expected %d but got %d", base, i, msg.Offset)
				}
				if strconv.Itoa(i) != string(msg.Key) {
					t.Errorf("wrong message key at loop %d...expected %d but got %s", base, i, string(msg.Key))
				}
				if values[i] != string(msg.Value) {
					t.Errorf("wrong message value at loop %d...expected %s but got %s", base, values[i], string(msg.Value))
				}
			}
		}
	})
}

func TestMixedCompressedMessages(t *testing.T) {
	t.Parallel()

	topic := kafka.CreateTopic(t, 1)

	offset := 0
	var values []string
	produce := func(n int, codec kafka.CompressionCodec) {
		w := kafka.NewWriter(kafka.WriterConfig{
			Brokers:          []string{"127.0.0.1:9092"},
			Topic:            topic,
			CompressionCodec: codec,
		})
		defer w.Close()

		msgs := make([]kafka.Message, n)
		for i := range msgs {
			value := fmt.Sprintf("Hello World %d!", offset)
			values = append(values, value)
			offset++
			msgs[i] = kafka.Message{Value: []byte(value)}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := w.WriteMessages(ctx, msgs...); err != nil {
			t.Errorf("failed to produce messages: %+v", err)
		}
	}

	// produce messages that interleave uncompressed messages and messages with
	// different compression codecs.  reader should be able to properly handle
	// all of them.
	produce(10, nil)
	produce(20, gzip.NewCompressionCodec())
	produce(5, nil)
	produce(10, snappy.NewCompressionCodec())
	produce(10, lz4.NewCompressionCodec())
	produce(5, nil)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"127.0.0.1:9092"},
		Topic:     topic,
		Partition: 0,
		MaxWait:   10 * time.Millisecond,
		MinBytes:  1,
		MaxBytes:  1024,
	})
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// in order to ensure proper handling of decompressing message, read at
	// offsets that we know to be in the middle of compressed message sets.
	for base := range values {
		r.SetOffset(int64(base))
		for i := base; i < len(values); i++ {
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				t.Errorf("error receiving message at loop %d, offset %d, reason: %+v", base, i, err)
			}
			if msg.Offset != int64(i) {
				t.Errorf("wrong offset at loop %d...expected %d but got %d", base, i, msg.Offset)
			}
			if values[i] != string(msg.Value) {
				t.Errorf("wrong message value at loop %d...expected %s but got %s", base, values[i], string(msg.Value))
			}
		}
	}
}

type noopCodec struct{}

func (noopCodec) Code() int8 {
	return 0
}

func (noopCodec) Encode(src []byte) ([]byte, error) {
	return src, nil
}

func (noopCodec) Decode(src []byte) ([]byte, error) {
	return src, nil
}

func BenchmarkCompression(b *testing.B) {
	benchmarks := []struct {
		scenario string
		codec    kafka.CompressionCodec
		function func(*testing.B, kafka.CompressionCodec, int, map[int][]byte)
	}{
		{
			scenario: "None",
			codec:    &noopCodec{},
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
		{
			scenario: "zstd",
			codec:    zstd.NewCompressionCodec(),
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
		Value: payload[payloadSize],
	}

	for i := 0; i < b.N; i++ {
		m1, err := codec.Encode(msg.Value)
		if err != nil {
			b.Fatal(err)
		}

		b.SetBytes(int64(len(m1)))

		_, err = codec.Decode(m1)
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
