package snappy

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/klauspost/compress/snappy"
	goxerialsnappy "github.com/segmentio/kafka-go/compress/snappy/go-xerial-snappy"
)

// Wrap an io.Reader or io.Writer to disable all copy optimizations like
// io.WriterTo or io.ReaderFrom.
// We use this to ensure writes are chunked by io.Copy's internal buffer
// in the tests.
type simpleReader struct{ io.Reader }
type simpleWriter struct{ io.Writer }

func TestXerialReaderSnappy(t *testing.T) {
	rawData := new(bytes.Buffer)
	rawData.Grow(1024 * 1024)
	io.CopyN(rawData, rand.Reader, 1024*1024)

	compressedRawData := bytes.NewReader(snappy.Encode(nil, rawData.Bytes()))

	decompressedData := new(bytes.Buffer)
	io.Copy(decompressedData,
		&xerialReader{reader: compressedRawData, decode: snappy.Decode})

	b0 := rawData.Bytes()
	b1 := decompressedData.Bytes()

	if !bytes.Equal(b0, b1) {
		t.Error("data mismatch")
	}
}

func TestXerialReaderWriter(t *testing.T) {
	rawData := new(bytes.Buffer)
	rawData.Grow(1024 * 1024)
	io.CopyN(rawData, rand.Reader, 1024*1024)

	framedData := new(bytes.Buffer)
	framedData.Grow(rawData.Len() + 1024)
	w := simpleWriter{&xerialWriter{writer: framedData}}
	r := simpleReader{bytes.NewReader(rawData.Bytes())}
	io.Copy(w, r)
	w.Writer.(*xerialWriter).Flush()

	unframedData := new(bytes.Buffer)
	unframedData.Grow(rawData.Len())
	io.Copy(unframedData, &xerialReader{reader: framedData})

	b0 := rawData.Bytes()
	b1 := unframedData.Bytes()

	if !bytes.Equal(b0, b1) {
		t.Error("data mismatch")
	}
}

func TestXerialFramedCompression(t *testing.T) {
	rawData := new(bytes.Buffer)
	rawData.Grow(1024 * 1024)
	io.CopyN(rawData, rand.Reader, 1024*1024)

	framedAndCompressedData := new(bytes.Buffer)
	framedAndCompressedData.Grow(rawData.Len())
	w := simpleWriter{&xerialWriter{writer: framedAndCompressedData, framed: true, encode: snappy.Encode}}
	r := simpleReader{bytes.NewReader(rawData.Bytes())}
	io.Copy(w, r)
	w.Writer.(*xerialWriter).Flush()

	unframedAndDecompressedData := new(bytes.Buffer)
	unframedAndDecompressedData.Grow(rawData.Len())
	io.Copy(unframedAndDecompressedData,
		simpleReader{&xerialReader{reader: framedAndCompressedData, decode: snappy.Decode}})

	b0 := rawData.Bytes()
	b1 := unframedAndDecompressedData.Bytes()

	if !bytes.Equal(b0, b1) {
		t.Error("data mismatch")
	}
}

func TestXerialFramedCompressionOptimized(t *testing.T) {
	rawData := new(bytes.Buffer)
	rawData.Grow(1024 * 1024)
	io.CopyN(rawData, rand.Reader, 1024*1024)

	framedAndCompressedData := new(bytes.Buffer)
	framedAndCompressedData.Grow(rawData.Len())
	w := &xerialWriter{writer: framedAndCompressedData, framed: true, encode: snappy.Encode}
	r := simpleReader{bytes.NewReader(rawData.Bytes())}
	io.Copy(w, r)
	w.Flush()

	unframedAndDecompressedData := new(bytes.Buffer)
	unframedAndDecompressedData.Grow(rawData.Len())
	io.Copy(unframedAndDecompressedData,
		&xerialReader{reader: framedAndCompressedData, decode: snappy.Decode})

	b0 := rawData.Bytes()
	b1 := unframedAndDecompressedData.Bytes()

	if !bytes.Equal(b0, b1) {
		t.Error("data mismatch")
	}
}

func TestXerialReaderAgainstGoXerialSnappy(t *testing.T) {
	rawData := new(bytes.Buffer)
	rawData.Grow(1024 * 1024)
	io.CopyN(rawData, rand.Reader, 1024*1024)
	rawBytes := rawData.Bytes()

	framedAndCompressedData := []byte{}
	const chunkSize = 999
	for i := 0; i < len(rawBytes); i += chunkSize {
		j := i + chunkSize
		if j > len(rawBytes) {
			j = len(rawBytes)
		}
		framedAndCompressedData = goxerialsnappy.EncodeStream(framedAndCompressedData, rawBytes[i:j])
	}

	unframedAndDecompressedData := new(bytes.Buffer)
	unframedAndDecompressedData.Grow(rawData.Len())
	io.Copy(unframedAndDecompressedData,
		&xerialReader{reader: bytes.NewReader(framedAndCompressedData), decode: snappy.Decode})

	b0 := rawBytes
	b1 := unframedAndDecompressedData.Bytes()

	if !bytes.Equal(b0, b1) {
		t.Error("data mismatch")
	}
}

func TestXerialWriterAgainstGoXerialSnappy(t *testing.T) {
	rawData := new(bytes.Buffer)
	rawData.Grow(1024 * 1024)
	io.CopyN(rawData, rand.Reader, 1024*1024)

	framedAndCompressedData := new(bytes.Buffer)
	framedAndCompressedData.Grow(rawData.Len())
	w := &xerialWriter{writer: framedAndCompressedData, framed: true, encode: snappy.Encode}
	r := simpleReader{bytes.NewReader(rawData.Bytes())}
	io.Copy(w, r)
	w.Flush()

	unframedAndDecompressedData, err := goxerialsnappy.Decode(framedAndCompressedData.Bytes())
	if err != nil {
		t.Error(err)
	}

	b0 := rawData.Bytes()
	b1 := unframedAndDecompressedData

	if !bytes.Equal(b0, b1) {
		t.Error("data mismatch")
	}
}
