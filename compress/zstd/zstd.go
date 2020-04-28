// Package zstd implements Zstandard compression.
package zstd

import (
	"io"
	"runtime"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Codec is the implementation of a compress.Codec which supports creating
// readers and writers for kafka messages compressed with zstd.
type Codec struct {
	// The compression level configured on writers created by the codec.
	//
	// Default to 3.
	Level int

	encPool sync.Pool
}

// Code implements the compress.Codec interface.
func (c *Codec) Code() int8 { return 4 }

// Name implements the compress.Codec interface.
func (c *Codec) Name() string { return "zstd" }

// NewReader implements the compress.Codec interface.
func (c *Codec) NewReader(r io.Reader) io.ReadCloser {
	p := new(reader)
	if cached := decPool.Get(); cached == nil {
		p.dec, p.err = zstd.NewReader(r)
		if p.dec != nil {
			// We need a finalizer because the reader spawns goroutines
			// that will only be stopped if the Close method is called.
			runtime.SetFinalizer(p, func(r *reader) { r.Close() })
		}
	} else {
		p.dec = cached.(*zstd.Decoder)
		p.err = p.dec.Reset(r)
	}
	return p
}

func (c *Codec) level() int {
	if c.Level != 0 {
		return c.Level
	}
	return 3
}

func (c *Codec) zstdLevel() zstd.EncoderLevel {
	return zstd.EncoderLevelFromZstd(c.level())
}

var decPool sync.Pool

type reader struct {
	dec *zstd.Decoder
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

// NewWriter implements the compress.Codec interface.
func (c *Codec) NewWriter(w io.Writer) io.WriteCloser {
	p := new(writer)
	if cached := c.encPool.Get(); cached == nil {
		p.enc, p.err = zstd.NewWriter(w, zstd.WithEncoderLevel(c.zstdLevel()))
		if p.enc != nil {
			// We need a finalizer because the writer spawns goroutines
			// that will only be stopped if the Close method is called.
			runtime.SetFinalizer(p, func(w *writer) { w.Close() })
		}
	} else {
		p.enc = cached.(*zstd.Encoder)
		p.enc.Reset(w)
	}
	p.c = c
	return p
}

type writer struct {
	c   *Codec
	enc *zstd.Encoder
	err error
}

// Close implements the io.Closer interface.
func (w *writer) Close() error {
	if w.enc == nil {
		return nil // already closed
	}
	err := w.enc.Close()
	w.c.encPool.Put(w.enc)
	w.c = nil
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
