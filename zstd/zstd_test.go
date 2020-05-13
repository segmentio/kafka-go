package zstd

import (
	"bytes"
	"crypto/rand"
	"github.com/segmentio/kafka-go"
	"io"
	"runtime"
	"testing"
)

func Test2(t *testing.T) {
	codec := NewCompressionCodec()

	// Run a compress/decompress cycle to create the first decoder and encoder.
	compressDecompress(codec)

	// Take a snapshot of the number of goroutines after the first comp+dec cycle.
	// Since all compression and decompression is serial and in this test case
	// goroutines can only go up if new decoders are created, this number is
	// expected to remain the same. If it doesn't, there's a leak.
	goroutinesBefore := runtime.NumGoroutine()

	// Stress the comp+dec cycle in serial fashion.
	for i := 0; i < 10000; i++ {
		compressDecompress(codec)
	}

	// There should be a single decoder in the pool.
	pooled := 0
	for dec := decPool.Get(); dec != nil; pooled, dec = pooled+1, decPool.Get() {
	}
	if pooled > 1 {
		t.Errorf("too many pooled decoders; expecting 1, got %d", pooled)
	}

	// And the number of goroutines shouldn't have gone up.
	goroutinesAfter := runtime.NumGoroutine()
	if goroutinesBefore != goroutinesAfter {
		t.Errorf("unexpected goroutine increase; expecting %d, got %d",
			goroutinesBefore, goroutinesAfter)
	}
}

func compressDecompress(codec *CompressionCodec) {
	data := randomData(2048)
	comp, _ := compress(codec, data)
	_, _ = decompress(codec, comp)
}

func randomData(len int) []byte {
	b := make([]byte, 0, len)
	_, _ = rand.Read(b)
	return b
}

func compress(codec kafka.CompressionCodec, src []byte) ([]byte, error) {
	b := new(bytes.Buffer)
	r := bytes.NewReader(src)
	w := codec.NewWriter(b)
	if _, err := io.Copy(w, r); err != nil {
		_ = w.Close()
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
		_ = r.Close()
		return nil, err
	}
	if err := r.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
