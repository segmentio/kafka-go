package snappy

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/golang/snappy"
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
	w := simpleWriter{&xerialWriter{writer: framedAndCompressedData, encode: snappy.Encode}}
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
	w := &xerialWriter{writer: framedAndCompressedData, encode: snappy.Encode}
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
