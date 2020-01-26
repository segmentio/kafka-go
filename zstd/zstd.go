// Package zstd implements Zstandard compression.
package zstd

import (
	"io"
	"sync"

	zstdlib "github.com/klauspost/compress/zstd"
	kafka "github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(NewCompressionCodec())
}

const Code = 4

const DefaultCompressionLevel = 3

type CompressionCodec struct{ level zstdlib.EncoderLevel }

func NewCompressionCodec() *CompressionCodec {
	return NewCompressionCodecWith(DefaultCompressionLevel)
}

func NewCompressionCodecWith(level int) *CompressionCodec {
	return &CompressionCodec{zstdlib.EncoderLevelFromZstd(level)}
}

// Code implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) Code() int8 { return Code }

// Name implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) Name() string { return "zstd" }

// NewReader implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) NewReader(r io.Reader) io.ReadCloser {
	p := new(reader)
	if cached := decPool.Get(); cached == nil {
		p.dec, p.err = zstdlib.NewReader(r)
	} else {
		p.dec = cached.(*zstdlib.Decoder)
		p.err = p.dec.Reset(r)
	}
	return p
}

var decPool sync.Pool

type reader struct {
	dec *zstdlib.Decoder
	err error
}

// Close implements the io.Closer interface.
func (r *reader) Close() error {
	if r.dec != nil {
		decPool.Put(r.dec)
		r.dec = nil
		r.err = io.ErrClosedPipe
	}
	return nil
}

// Read implements the io.Reader interface.
func (r *reader) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	return r.dec.Read(p)
}

// WriteTo implements the io.WriterTo interface.
func (r *reader) WriteTo(w io.Writer) (n int64, err error) {
	if r.err != nil {
		return 0, r.err
	}
	return r.dec.WriteTo(w)
}

// NewWriter implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) NewWriter(w io.Writer) io.WriteCloser {
	p := new(writer)
	if cached := encPool.Get(); cached == nil {
		p.enc, p.err = zstdlib.NewWriter(w,
			zstdlib.WithEncoderLevel(c.level))
	} else {
		p.enc = cached.(*zstdlib.Encoder)
		p.enc.Reset(w)
	}
	return p
}

var encPool sync.Pool

type writer struct {
	enc *zstdlib.Encoder
	err error
}

// Close implements the io.Closer interface.
func (w *writer) Close() error {
	if w.enc == nil {
		return nil // already closed
	}
	err := w.enc.Close()
	encPool.Put(w.enc)
	w.enc = nil
	w.err = io.ErrClosedPipe
	return err
}

// WriteTo implements the io.WriterTo interface.
func (w *writer) Write(p []byte) (n int, err error) {
	if w.err != nil {
		return 0, w.err
	}
	return w.enc.Write(p)
}

// ReadFrom implements the io.ReaderFrom interface.
func (w *writer) ReadFrom(r io.Reader) (n int64, err error) {
	if w.err != nil {
		return 0, w.err
	}
	return w.enc.ReadFrom(r)
}
