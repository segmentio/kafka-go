package compress_test

import (
	"bytes"
	stdgzip "compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"text/tabwriter"
	"time"

	kafka "github.com/segmentio/kafka-go"
	pkg "github.com/segmentio/kafka-go/compress"
	"github.com/segmentio/kafka-go/compress/gzip"
	"github.com/segmentio/kafka-go/compress/lz4"
	"github.com/segmentio/kafka-go/compress/snappy"
	"github.com/segmentio/kafka-go/compress/zstd"
	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestCodecs(t *testing.T) {
	for i, c := range pkg.Codecs {
		if c != nil {
			if code := c.Code(); int8(code) != int8(i) {
				t.Fatal("default compression codec table is misconfigured for", c.Name())
			}
		}
	}
}

func TestCompression(t *testing.T) {
	msg := kafka.Message{
		Value: []byte("message"),
	}

	testEncodeDecode(t, msg, new(gzip.Codec))
	testEncodeDecode(t, msg, new(snappy.Codec))
	testEncodeDecode(t, msg, new(lz4.Codec))
	if ktesting.KafkaIsAtLeast("2.1.0") {
		testEncodeDecode(t, msg, new(zstd.Codec))
	}
}

func compress(codec pkg.Codec, src []byte) ([]byte, error) {
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

func decompress(codec pkg.Codec, src []byte) ([]byte, error) {
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

func testEncodeDecode(t *testing.T, m kafka.Message, codec pkg.Codec) {
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
	testCompressedMessages(t, new(gzip.Codec))
	testCompressedMessages(t, new(snappy.Codec))
	testCompressedMessages(t, new(lz4.Codec))

	if ktesting.KafkaIsAtLeast("2.1.0") {
		testCompressedMessages(t, new(zstd.Codec))
	}
}

func testCompressedMessages(t *testing.T, codec pkg.Codec) {
	t.Run("produce/consume with"+codec.Name(), func(t *testing.T) {
		t.Parallel()

		topic := createTopic(t, 1)
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

	topic := createTopic(t, 1)

	offset := 0
	var values []string
	produce := func(n int, codec pkg.Codec) {
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
	produce(20, new(gzip.Codec))
	produce(5, nil)
	produce(10, new(snappy.Codec))
	produce(10, new(lz4.Codec))
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
		codec    pkg.Codec
		function func(*testing.B, pkg.Codec, *bytes.Buffer, []byte) float64
	}{
		{
			codec:    &noopCodec{},
			function: benchmarkCompression,
		},
		{
			codec:    new(gzip.Codec),
			function: benchmarkCompression,
		},
		{
			codec:    new(snappy.Codec),
			function: benchmarkCompression,
		},
		{
			codec:    new(lz4.Codec),
			function: benchmarkCompression,
		},
		{
			codec:    new(zstd.Codec),
			function: benchmarkCompression,
		},
	}

	f, err := os.Open(filepath.Join(os.Getenv("GOROOT"), "src/encoding/json/testdata/code.json.gz"))
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	z, err := stdgzip.NewReader(f)
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

func benchmarkCompression(b *testing.B, codec pkg.Codec, buf *bytes.Buffer, payload []byte) float64 {
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

func makeTopic() string {
	return fmt.Sprintf("kafka-go-%016x", rand.Int63())
}

func createTopic(t *testing.T, partitions int) string {
	topic := makeTopic()

	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})

	switch err {
	case nil:
		// ok
	case kafka.TopicAlreadyExists:
		// ok
	default:
		t.Error("bad createTopics", err)
		t.FailNow()
	}

	return topic
}
