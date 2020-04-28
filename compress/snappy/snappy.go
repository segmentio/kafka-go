package snappy

import (
	"io"
	"sync"

	"github.com/golang/snappy"
)

// Framing is an enumeration type used to enable or disable xerial framing of
// snappy messages.
type Framing int

const (
	Framed Framing = iota
	Unframed
)

// Codec is the implementation of a compress.Codec which supports creating
// readers and writers for kafka messages compressed with snappy.
type Codec struct {
	// An optional framing to apply to snappy compression.
	//
	// Default to Framed.
	Framing Framing
}

// Code implements the compress.Codec interface.
func (c *Codec) Code() int8 { return 2 }

// Name implements the compress.Codec interface.
func (c *Codec) Name() string { return "snappy" }

// NewReader implements the compress.Codec interface.
func (c *Codec) NewReader(r io.Reader) io.ReadCloser {
	x := readerPool.Get().(*xerialReader)
	x.Reset(r)
	return &reader{x}
}

// NewWriter implements the compress.Codec interface.
func (c *Codec) NewWriter(w io.Writer) io.WriteCloser {
	x := writerPool.Get().(*xerialWriter)
	x.Reset(w)
	x.framed = c.Framing == Framed
	return &writer{x}
}

type reader struct{ *xerialReader }

func (r *reader) Close() (err error) {
	if x := r.xerialReader; x != nil {
		r.xerialReader = nil
		x.Reset(nil)
		readerPool.Put(x)
	}
	return
}

type writer struct{ *xerialWriter }

func (w *writer) Close() (err error) {
	if x := w.xerialWriter; x != nil {
		w.xerialWriter = nil
		err = x.Flush()
		x.Reset(nil)
		writerPool.Put(x)
	}
	return
}

var readerPool = sync.Pool{
	New: func() interface{} {
		return &xerialReader{decode: snappy.Decode}
	},
}

var writerPool = sync.Pool{
	New: func() interface{} {
		return &xerialWriter{encode: snappy.Encode}
	},
}
