package compress_test

import (
	"bytes"
	stdgzip "compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"text/tabwriter"
	"time"

	kafka "github.com/apoorvag-mav/kafka-go"
	pkg "github.com/apoorvag-mav/kafka-go/compress"
	"github.com/apoorvag-mav/kafka-go/compress/gzip"
	"github.com/apoorvag-mav/kafka-go/compress/lz4"
	"github.com/apoorvag-mav/kafka-go/compress/snappy"
	"github.com/apoorvag-mav/kafka-go/compress/zstd"
	ktesting "github.com/apoorvag-mav/kafka-go/testing"
)

func init() {
	// Seeding the random source is important to prevent multiple test runs from
	// reusing the same topic names.
	rand.Seed(time.Now().UnixNano())
}

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
			t.Fatal(err)
		}
	})

	t.Run("decode with "+codec.Name(), func(t *testing.T) {
		if r1 == nil {
			if r1, err = compress(codec, m.Value); err != nil {
				t.Fatal(err)
			}
		}
		r2, err = decompress(codec, r1)
		if err != nil {
			t.Fatal(err)
		}
		if string(r2) != "message" {
			t.Error("bad message")
			t.Logf("expected: %q", string(m.Value))
			t.Logf("got:      %q", string(r2))
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
	t.Run(codec.Name(), func(t *testing.T) {
		client, topic, shutdown := newLocalClientAndTopic()
		defer shutdown()

		w := &kafka.Writer{
			Addr:         kafka.TCP("127.0.0.1:9092"),
			Topic:        topic,
			Compression:  kafka.Compression(codec.Code()),
			BatchTimeout: 10 * time.Millisecond,
			Transport:    client.Transport,
		}
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
					t.Fatalf("error receiving message at loop %d, offset %d, reason: %+v", base, i, err)
				}
				if msg.Offset != int64(i) {
					t.Fatalf("wrong offset at loop %d...expected %d but got %d", base, i, msg.Offset)
				}
				if strconv.Itoa(i) != string(msg.Key) {
					t.Fatalf("wrong message key at loop %d...expected %d but got %s", base, i, string(msg.Key))
				}
				if values[i] != string(msg.Value) {
					t.Fatalf("wrong message value at loop %d...expected %s but got %s", base, values[i], string(msg.Value))
				}
			}
		}
	})
}

func TestMixedCompressedMessages(t *testing.T) {
	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	offset := 0
	var values []string
	produce := func(n int, codec pkg.Codec) {
		w := &kafka.Writer{
			Addr:      kafka.TCP("127.0.0.1:9092"),
			Topic:     topic,
			Transport: client.Transport,
		}
		defer w.Close()

		if codec != nil {
			w.Compression = kafka.Compression(codec.Code())
		}

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

func init() {
	rand.Seed(time.Now().UnixNano())
}

func makeTopic() string {
	return fmt.Sprintf("kafka-go-%016x", rand.Int63())
}

func newLocalClientAndTopic() (*kafka.Client, string, func()) {
	topic := makeTopic()
	client, shutdown := newLocalClient()

	_, err := client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}},
	})
	if err != nil {
		shutdown()
		panic(err)
	}

	// Topic creation seems to be asynchronous. Metadata for the topic partition
	// layout in the cluster is available in the controller before being synced
	// with the other brokers, which causes "Error:[3] Unknown Topic Or Partition"
	// when sending requests to the partition leaders.
	for i := 0; i < 20; i++ {
		r, err := client.Fetch(context.Background(), &kafka.FetchRequest{
			Topic:     topic,
			Partition: 0,
			Offset:    0,
		})
		if err == nil && r.Error == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	return client, topic, func() {
		client.DeleteTopics(context.Background(), &kafka.DeleteTopicsRequest{
			Topics: []string{topic},
		})
		shutdown()
	}
}

func newLocalClient() (*kafka.Client, func()) {
	return newClient(kafka.TCP("127.0.0.1:9092"))
}

func newClient(addr net.Addr) (*kafka.Client, func()) {
	conns := &ktesting.ConnWaitGroup{
		DialFunc: (&net.Dialer{}).DialContext,
	}

	transport := &kafka.Transport{
		Dial: conns.Dial,
	}

	client := &kafka.Client{
		Addr:      addr,
		Timeout:   5 * time.Second,
		Transport: transport,
	}

	return client, func() { transport.CloseIdleConnections(); conns.Wait() }
}
