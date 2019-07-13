// +build cgo

package zstd

import (
	"io"
	"sync"

	"github.com/DataDog/zstd"
	kafka "github.com/segmentio/kafka-go"
)

func init() {
	kafka.RegisterCompressionCodec(NewCompressionCodec())
}

const (
	Code = 4

	DefaultCompressionLevel = zstd.DefaultCompression
)

type CompressionCodec struct{ level int }

func NewCompressionCodec() *CompressionCodec {
	return NewCompressionCodecWith(DefaultCompressionLevel)
}

func NewCompressionCodecWith(level int) *CompressionCodec {
	return &CompressionCodec{level}
}

// Code implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) Code() int8 { return Code }

// Name implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) Name() string { return "zstd" }

// NewReader implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) NewReader(r io.Reader) io.ReadCloser {
	return &reader{
		reader: r,
		buffer: bufferPool.Get().(*buffer),
	}
}

// NewWriter implements the kafka.CompressionCodec interface.
func (c *CompressionCodec) NewWriter(w io.Writer) io.WriteCloser {
	return &writer{
		writer: w,
		buffer: bufferPool.Get().(*buffer),
		level:  c.level,
	}
}

// =============================================================================
// The DataDog/zstd package exposes io.Writer and io.Reader implementations that
// encode and decode streams, however there are no APIs to reuse the values like
// other compression format have (through a Reset method usually).
//
// I first tried using these abstractions but the amount of state that gets
// recreated and destroyed was so large that it was slower than using the
// zstd.Compress and zstd.Decompress functions directly. Knowing that, I changed
// the implementation to be more of a buffer management on top of these instead.
// =============================================================================

type reader struct {
	reader io.Reader
	buffer *buffer
	offset int
}

func (r *reader) Read(b []byte) (int, error) {
	if err := r.decompress(); err != nil {
		return 0, err
	}

	if r.offset >= len(r.buffer.output) {
		return 0, io.EOF
	}

	n := copy(b, r.buffer.output[r.offset:])
	r.offset += n
	return n, nil
}

func (r *reader) WriteTo(w io.Writer) (int64, error) {
	if err := r.decompress(); err != nil {
		return 0, err
	}

	if r.offset >= len(r.buffer.output) {
		return 0, nil
	}

	n, err := w.Write(r.buffer.output[r.offset:])
	r.offset += n
	return int64(n), err
}

func (r *reader) Close() (err error) {
	if b := r.buffer; b != nil {
		r.buffer = nil
		b.reset()
		bufferPool.Put(b)
	}
	return
}

func (r *reader) decompress() (err error) {
	if r.reader == nil {
		return
	}

	b := r.buffer

	if _, err = b.readFrom(r.reader); err != nil {
		return
	}

	r.reader = nil
	b.output, err = zstd.Decompress(b.output[:cap(b.output)], b.input)
	return
}

type writer struct {
	writer io.Writer
	buffer *buffer
	level  int
}

func (w *writer) Write(b []byte) (int, error) {
	return w.buffer.write(b)
}

func (w *writer) ReadFrom(r io.Reader) (int64, error) {
	return w.buffer.readFrom(r)
}

func (w *writer) Close() (err error) {
	if b := w.buffer; b != nil {
		w.buffer = nil

		b.output, err = zstd.CompressLevel(b.output[:cap(b.output)], b.input, w.level)
		if err == nil {
			_, err = w.writer.Write(b.output)
		}

		b.reset()
		bufferPool.Put(b)
	}
	return
}

type buffer struct {
	input  []byte
	output []byte
}

func (b *buffer) reset() {
	b.input = b.input[:0]
	b.output = b.output[:0]
}

func (b *buffer) readFrom(r io.Reader) (int64, error) {
	prefix := len(b.input)

	for {
		if len(b.input) == cap(b.input) {
			tmp := make([]byte, len(b.input), 2*cap(b.input))
			copy(tmp, b.input)
			b.input = tmp
		}

		n, err := r.Read(b.input[len(b.input):cap(b.input)])
		b.input = b.input[:len(b.input)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return int64(len(b.input) - prefix), err
		}
	}
}

func (b *buffer) write(data []byte) (int, error) {
	b.input = append(b.input, data...)
	return len(data), nil
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &buffer{
			input:  make([]byte, 0, 32*1024),
			output: make([]byte, 0, 32*1024),
		}
	},
}
