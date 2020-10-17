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

	encoderPool sync.Pool // *encoder
}

// Code implements the compress.Codec interface.
func (c *Codec) Code() int8 { return 4 }

// Name implements the compress.Codec interface.
func (c *Codec) Name() string { return "zstd" }

// NewReader implements the compress.Codec interface.
func (c *Codec) NewReader(r io.Reader) io.ReadCloser {
	p := new(reader)
	if cached := decoderPool.Get(); cached == nil {
		z, err := zstd.NewReader(r)
		if err != nil {
			p.err = err
		} else {
			p.dec = &decoder{z}
			// We need a finalizer because the reader spawns goroutines
			// that will only be stopped if the Close method is called.
			runtime.SetFinalizer(p.dec, (*decoder).finalize)
		}
	} else {
		p.dec = cached.(*decoder)
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

var decoderPool sync.Pool // *decoder

type decoder struct {
	*zstd.Decoder
}

func (d *decoder) finalize() {
	d.Close()
}

type reader struct {
	dec *decoder
	err error
}

// Close implements the io.Closer interface.
func (r *reader) Close() error {
	if r.dec != nil {
		decoderPool.Put(r.dec)
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
	if cached := c.encoderPool.Get(); cached == nil {
		z, err := zstd.NewWriter(w, zstd.WithEncoderLevel(c.zstdLevel()))
		if err != nil {
			p.err = err
		} else {
			p.enc = &encoder{z}
			// We need a finalizer because the writer spawns goroutines
			// that will only be stopped if the Close method is called.
			runtime.SetFinalizer(p.enc, (*encoder).finalize)
		}
	} else {
		p.enc = cached.(*encoder)
		p.enc.Reset(w)
	}
	p.c = c
	return p
}

type encoder struct {
	*zstd.Encoder
}

func (e *encoder) finalize() {
	e.Close()
}

type writer struct {
	c   *Codec
	enc *encoder
	err error
}

// Close implements the io.Closer interface.
func (w *writer) Close() error {
	if w.enc != nil {
		w.c.encoderPool.Put(w.enc)
	}
	if w.err == nil {
		w.err = io.ErrClosedPipe
	}
	return w.err
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
