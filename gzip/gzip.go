package gzip

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"sync"

	kafka "github.com/segmentio/kafka-go"
)

var (
	// emptyGzipBytes is the binary value for an empty file that has been
	// gzipped.  It is used to initialize gzip.Reader before adding it to the
	// readerPool.
	emptyGzipBytes = [...]byte{
		0x1f, 0x8b, 0x08, 0x08, 0x0d, 0x0c, 0x67, 0x5c, 0x00, 0x03, 0x66, 0x6f,
		0x6f, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}

	readerPool = sync.Pool{
		New: func() interface{} {
			// if the reader doesn't get valid gzip at initialization time,
			// it will not be valid and will fail on Reset.
			reader, _ := gzip.NewReader(newEmptyGzipFile())
			return reader
		},
	}

	writerPool = sync.Pool{
		New: func() interface{} {
			return gzip.NewWriter(bytes.NewBuffer(nil))
		},
	}
)

func newEmptyGzipFile() io.Reader {
	return bytes.NewReader(emptyGzipBytes[:])
}

func init() {
	kafka.RegisterCompressionCodec(func() kafka.CompressionCodec {
		return NewCompressionCodec()
	})
}

type CompressionCodec struct {
	// CompressionLevel is the level of compression to use on messages.
	CompressionLevel int
}

const (
	Code                    int8 = 1
	DefaultCompressionLevel int  = -1
)

func NewCompressionCodec() CompressionCodec {
	return NewCompressionCodecWith(DefaultCompressionLevel)
}

func NewCompressionCodecWith(level int) CompressionCodec {
	return CompressionCodec{
		CompressionLevel: level,
	}
}

// Code implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Code() int8 {
	return Code
}

// Encode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Encode(src []byte) ([]byte, error) {
	buf := bytes.Buffer{}
	buf.Grow(len(src)) // guess a size to avoid repeat allocations.
	writer := writerPool.Get().(*gzip.Writer)
	writer.Reset(&buf)
	defer writerPool.Put(writer)
	defer writer.Reset(nil)

	_, err := writer.Write(src)
	if err != nil {
		return nil, err
	}

	// note that the gzip reader must be closed in order for it to write
	// out trailing contents.  Flush is insufficient.  it is okay to re-use
	// the writer even after it's closed by Resetting it.
	err = writer.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

// Decode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Decode(src []byte) ([]byte, error) {
	reader := readerPool.Get().(*gzip.Reader)
	defer readerPool.Put(reader)
	defer reader.Reset(newEmptyGzipFile())
	err := reader.Reset(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(reader)
}

// NewReader implements the kafka.CompressionCodec interface.
func (c CompressionCodec) NewReader(r io.Reader) io.ReadCloser {
	z := readerPool.Get().(*gzip.Reader)
	z.Reset(r)
	return &reader{z}
}

// NewWriter implements the kafka.CompressionCodec interface.
func (c CompressionCodec) NewWriter(w io.Writer) io.WriteCloser {
	z := writerPool.Get().(*gzip.Writer)
	z.Reset(w)
	return &writer{z}
}

type reader struct{ *gzip.Reader }

func (r *reader) Close() (err error) {
	if z := r.Reader; z != nil {
		r.Reader = nil
		err = z.Close()
		z.Reset(newEmptyGzipFile())
		readerPool.Put(z)
	}
	return
}

type writer struct{ *gzip.Writer }

func (w *writer) Close() (err error) {
	if z := w.Writer; z != nil {
		w.Writer = nil
		err = z.Close()
		z.Reset(nil)
		writerPool.Put(z)
	}
	return
}
