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
			reader := &gzipReader{}
			reader.Reset(nil)
			return reader
		},
	}
)

type gzipReader struct {
	gzip.Reader
	emptyGzipFile bytes.Reader
}

func (z *gzipReader) Reset(r io.Reader) {
	if r == nil {
		z.emptyGzipFile.Reset(emptyGzipBytes[:])
		r = &z.emptyGzipFile
	}
	z.Reader.Reset(r)
}

func init() {
	kafka.RegisterCompressionCodec(NewCompressionCodec())
}

const (
	Code = 1

	DefaultCompressionLevel = gzip.DefaultCompression
)

type CompressionCodec struct{ writerPool sync.Pool }

func NewCompressionCodec() *CompressionCodec {
	return NewCompressionCodecLevel(DefaultCompressionLevel)
}

func NewCompressionCodecLevel(level int) *CompressionCodec {
	return &CompressionCodec{
		writerPool: sync.Pool{
			New: func() interface{} {
				w, err := gzip.NewWriterLevel(ioutil.Discard, level)
				if err != nil {
					return err
				}
				return w
			},
		},
	}
}

// Code implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) Code() int8 { return Code }

// Name implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) Name() string { return "gzip" }

// NewReader implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) NewReader(r io.Reader) io.ReadCloser {
	z := readerPool.Get().(*gzipReader)
	z.Reset(r)
	return &reader{z}
}

// NewWriter implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) NewWriter(w io.Writer) io.WriteCloser {
	x := c.writerPool.Get()
	z, _ := x.(*gzip.Writer)
	if z == nil {
		return errorWriter{err: x.(error)}
	}
	z.Reset(w)
	return &writer{c, z}
}

type reader struct{ *gzipReader }

func (r *reader) Close() (err error) {
	if z := r.gzipReader; z != nil {
		r.gzipReader = nil
		err = z.Close()
		z.Reset(nil)
		readerPool.Put(z)
	}
	return
}

type writer struct {
	c *CompressionCodec
	*gzip.Writer
}

func (w *writer) Close() (err error) {
	if z := w.Writer; z != nil {
		w.Writer = nil
		err = z.Close()
		z.Reset(nil)
		w.c.writerPool.Put(z)
	}
	return
}

type errorWriter struct{ err error }

func (w errorWriter) Close() error { return w.err }

func (w errorWriter) Write(b []byte) (int, error) { return 0, w.err }
