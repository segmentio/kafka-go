package snappy

import (
	"io"
	"sync"

	"github.com/golang/snappy"
	kafka "github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(NewCompressionCodec())
}

// Framing is an enumeration type used to enable or disable xerial framing of
// snappy messages.
type Framing int

const (
	Framed Framing = iota
	Unframed
)

const (
	Code = 2
)

type CompressionCodec struct{ framing Framing }

func NewCompressionCodec() *CompressionCodec {
	return NewCompressionCodecFraming(Framed)
}

func NewCompressionCodecFraming(framing Framing) *CompressionCodec {
	return &CompressionCodec{framing}
}

// Code implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) Code() int8 { return Code }

// Name implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) Name() string { return "snappy" }

// NewReader implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) NewReader(r io.Reader) io.ReadCloser {
	x := readerPool.Get().(*xerialReader)
	x.Reset(r)
	return &reader{x}
}

// NewWriter implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) NewWriter(w io.Writer) io.WriteCloser {
	x := writerPool.Get().(*xerialWriter)
	x.Reset(w)
	x.framed = c.framing == Framed
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
