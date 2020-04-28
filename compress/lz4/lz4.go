package lz4

import (
	"io"
	"sync"

	"github.com/pierrec/lz4"
)

// Codec is the implementation of a compress.Codec which supports creating
// readers and writers for kafka messages compressed with lz4.
type Codec struct{}

// Code implements the compress.Codec interface.
func (c *Codec) Code() int8 { return 3 }

// Name implements the compress.Codec interface.
func (c *Codec) Name() string { return "lz4" }

// NewReader implements the compress.Codec interface.
func (c *Codec) NewReader(r io.Reader) io.ReadCloser {
	z := readerPool.Get().(*lz4.Reader)
	z.Reset(r)
	return &reader{z}
}

// NewWriter implements the compress.Codec interface.
func (c *Codec) NewWriter(w io.Writer) io.WriteCloser {
	z := writerPool.Get().(*lz4.Writer)
	z.Reset(w)
	return &writer{z}
}

type reader struct{ *lz4.Reader }

func (r *reader) Close() (err error) {
	if z := r.Reader; z != nil {
		r.Reader = nil
		z.Reset(nil)
		readerPool.Put(z)
	}
	return
}

type writer struct{ *lz4.Writer }

func (w *writer) Close() (err error) {
	if z := w.Writer; z != nil {
		w.Writer = nil
		err = z.Close()
		z.Reset(nil)
		writerPool.Put(z)
	}
	return
}

var readerPool = sync.Pool{
	New: func() interface{} { return lz4.NewReader(nil) },
}

var writerPool = sync.Pool{
	New: func() interface{} { return lz4.NewWriter(nil) },
}
