package kafka_test

import (
	"bytes"
	compressGzip "compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"text/tabwriter"
	"time"

	kafka "github.com/segmentio/kafka-go"
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

func compress(codec kafka.CompressionCodec, src []byte) ([]byte, error) {
	b := new(bytes.Buffer)
	r := bytes.NewReader(src)
	w := codec.NewWriter(b)
	if _, err := io.Copy(w, r); err != nil {
		w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func decompress(codec kafka.CompressionCodec, src []byte) ([]byte, error) {
	b := new(bytes.Buffer)
	r := codec.NewReader(bytes.NewReader(src))
	if _, err := io.Copy(b, r); err != nil {
		r.Close()
		return nil, err
	}
	if err := r.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func testEncodeDecode(t *testing.T, m kafka.Message, codec kafka.CompressionCodec) {
	var r1, r2 []byte
	var err error

	t.Run("encode with "+codec.Name(), func(t *testing.T) {
		r1, err = compress(codec, m.Value)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("decode with "+codec.Name(), func(t *testing.T) {
		r2, err = decompress(codec, r1)
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

func TestCompressedMessages(t *testing.T) {
	testCompressedMessages(t, gzip.NewCompressionCodec())
	testCompressedMessages(t, snappy.NewCompressionCodec())
	testCompressedMessages(t, lz4.NewCompressionCodec())

	if ktesting.KafkaIsAtLeast("2.1.0") {
		testCompressedMessages(t, zstd.NewCompressionCodec())
	}
}

func testCompressedMessages(t *testing.T, codec kafka.CompressionCodec) {
	t.Run("produce/consume with"+codec.Name(), func(t *testing.T) {
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

func (noopCodec) Name() string {
	return "none"
}

func (noopCodec) NewReader(r io.Reader) io.ReadCloser {
	return ioutil.NopCloser(r)
}

func (noopCodec) NewWriter(w io.Writer) io.WriteCloser {
	return nopWriteCloser{w}
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func BenchmarkCompression(b *testing.B) {
	benchmarks := []struct {
		codec    kafka.CompressionCodec
		function func(*testing.B, kafka.CompressionCodec, *bytes.Buffer, []byte) float64
	}{
		{
			codec:    &noopCodec{},
			function: benchmarkCompression,
		},
		{
			codec:    gzip.NewCompressionCodec(),
			function: benchmarkCompression,
		},
		{
			codec:    snappy.NewCompressionCodec(),
			function: benchmarkCompression,
		},
		{
			codec:    lz4.NewCompressionCodec(),
			function: benchmarkCompression,
		},
		{
			codec:    zstd.NewCompressionCodec(),
			function: benchmarkCompression,
		},
	}

	f, err := os.Open(filepath.Join(os.Getenv("GOROOT"), "src/encoding/json/testdata/code.json.gz"))
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	z, err := compressGzip.NewReader(f)
	if err != nil {
		b.Fatal(err)
	}

	payload, err := ioutil.ReadAll(z)
	if err != nil {
		b.Fatal(err)
	}

	buffer := bytes.Buffer{}
	buffer.Grow(len(payload))

	ts := &bytes.Buffer{}
	tw := tabwriter.NewWriter(ts, 0, 8, 0, '\t', 0)
	defer func() {
		tw.Flush()
		fmt.Printf("input => %.2f MB\n", float64(len(payload))/(1024*1024))
		fmt.Println(ts)
	}()

	b.ResetTimer()

	for i := range benchmarks {
		benchmark := &benchmarks[i]
		ratio := 0.0

		b.Run(fmt.Sprintf("%s", benchmark.codec.Name()), func(b *testing.B) {
			ratio = benchmark.function(b, benchmark.codec, &buffer, payload)
		})

		fmt.Fprintf(tw, "  %s:\t%.2f%%\n", benchmark.codec.Name(), 100*ratio)
	}
}

func benchmarkCompression(b *testing.B, codec kafka.CompressionCodec, buf *bytes.Buffer, payload []byte) float64 {
	// In case only the decompression benchmark are run, we use this flags to
	// detect whether we have to compress the payload before the decompression
	// benchmarks.
	compressed := false

	b.Run("compress", func(b *testing.B) {
		compressed = true
		r := bytes.NewReader(payload)

		for i := 0; i < b.N; i++ {
			buf.Reset()
			r.Reset(payload)
			w := codec.NewWriter(buf)

			_, err := io.Copy(w, r)
			if err != nil {
				b.Fatal(err)
			}
			if err := w.Close(); err != nil {
				b.Fatal(err)
			}
		}

		b.SetBytes(int64(buf.Len()))
	})

	if !compressed {
		r := bytes.NewReader(payload)
		w := codec.NewWriter(buf)

		_, err := io.Copy(w, r)
		if err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("decompress", func(b *testing.B) {
		c := bytes.NewReader(buf.Bytes())

		for i := 0; i < b.N; i++ {
			c.Reset(buf.Bytes())
			r := codec.NewReader(c)

			n, err := io.Copy(ioutil.Discard, r)
			if err != nil {
				b.Fatal(err)
			}
			if err := r.Close(); err != nil {
				b.Fatal(err)
			}

			b.SetBytes(n)
		}
	})

	return 1 - (float64(buf.Len()) / float64(len(payload)))
}
