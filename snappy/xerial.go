package snappy

import (
	"bytes"
	"encoding/binary"
	"io"
)

// An implementation of io.Reader which consumes a stream of xerial-framed
// snappy-encoeded data. The framing is optional, if no framing is detected
// the reader will simply forward the bytes from its underlying stream.
type xerialReader struct {
	reader io.Reader
	header [20]byte
	input  []byte
	output []byte
	offset int
	decode func([]byte, []byte) ([]byte, error)
}

func (x *xerialReader) Reset(r io.Reader) {
	x.reader = r
	x.input = x.input[:0]
	x.output = x.output[:0]
	x.offset = 0
}

func (x *xerialReader) Read(b []byte) (int, error) {
	for {
		if x.offset < len(x.output) {
			n := copy(b, x.output[x.offset:])
			x.offset += n
			return n, nil
		}

		if err := x.readChunk(); err != nil {
			return 0, err
		}
	}
}

func (x *xerialReader) WriteTo(w io.Writer) (int64, error) {
	wn := int64(0)

	for {
		for x.offset < len(x.output) {
			n, err := w.Write(x.output[x.offset:])
			wn += int64(n)
			x.offset += n
			if err != nil {
				return wn, err
			}
		}

		if err := x.readChunk(); err != nil {
			if err == io.EOF {
				err = nil
			}
			return wn, err
		}
	}
}

func (x *xerialReader) readChunk() error {
	x.offset = 0

	n, err := io.ReadFull(x.reader, x.header[:])
	if err != nil && n == 0 {
		return err
	}

	if n == len(x.header) && isXerialHeader(x.header[:]) {
		n := int(binary.BigEndian.Uint32(x.header[16:]))

		if cap(x.input) < n {
			x.input = make([]byte, n, align(n, 1024))
		} else {
			x.input = x.input[:n]
		}

		_, err := io.ReadFull(x.reader, x.input)
		if err != nil {
			return err
		}
	} else {
		x.input = append(x.input[:0], x.header[:n]...)

		for {
			if len(x.input) == cap(x.input) {
				b := make([]byte, len(x.input), 2*cap(x.input))
				copy(b, x.input)
				x.input = b
			}

			n, err := x.reader.Read(x.input[len(x.input):cap(x.input)])
			x.input = x.input[:len(x.input)+n]
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
		}
	}

	if x.decode == nil {
		x.output, x.input = x.input, x.output
	} else {
		b, err := x.decode(x.output[:cap(x.output)], x.input)
		if err != nil {
			return err
		}
		x.output = b
	}

	return nil
}

// An implementation of a xerial-framed snappy-encoded output stream.
// Each Write made to the writer is framed with a xerial header.
type xerialWriter struct {
	writer io.Writer
	header [20]byte
	input  []byte
	output []byte
	encode func([]byte, []byte) []byte
}

func (x *xerialWriter) Reset(w io.Writer) {
	x.writer = w
	x.input = x.input[:0]
	x.output = x.output[:0]
}

func (x *xerialWriter) ReadFrom(r io.Reader) (int64, error) {
	wn := int64(0)

	if cap(x.input) == 0 {
		x.input = make([]byte, 0, 32*1024)
	}

	for {
		n, err := r.Read(x.input[len(x.input):cap(x.input)])
		wn += int64(n)
		x.input = x.input[:len(x.input)+n]

		if x.fullEnough() {
			if err := x.Flush(); err != nil {
				return wn, err
			}
		}

		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return wn, err
		}
	}
}

func (x *xerialWriter) Write(b []byte) (int, error) {
	wn := 0

	if cap(x.input) == 0 {
		x.input = make([]byte, 0, 32*1024)
	}

	for len(b) > 0 {
		n := copy(x.input[len(x.input):cap(x.input)], b)
		b = b[n:]
		wn += n
		x.input = x.input[:len(x.input)+n]

		if x.fullEnough() {
			if err := x.Flush(); err != nil {
				return wn, err
			}
		}
	}

	return wn, nil
}

func (x *xerialWriter) Flush() error {
	if len(x.input) == 0 {
		return nil
	}

	var b []byte
	if x.encode == nil {
		b = x.input
	} else {
		x.output = x.encode(x.output[:cap(x.output)], x.input)
		b = x.output
	}

	x.input = x.input[:0]
	x.output = x.output[:0]
	writeXerialHeader(x.header[:], len(b))

	if _, err := x.writer.Write(x.header[:]); err != nil {
		return err
	}

	_, err := x.writer.Write(b)
	return err
}

func (x *xerialWriter) fullEnough() bool {
	return (cap(x.input) - len(x.input)) < 1024
}

func align(n, a int) int {
	if (n % a) == 0 {
		return n
	}
	return ((n / a) + 1) * a
}

var (
	xerialVersionInfo = [...]byte{0, 0, 0, 1, 0, 0, 0, 1}
	xerialHeader      = [...]byte{130, 83, 78, 65, 80, 80, 89, 0}
)

func isXerialHeader(src []byte) bool {
	return len(src) >= 20 && bytes.Equal(src[:8], xerialHeader[:])
}

func writeXerialHeader(b []byte, n int) {
	copy(b[:8], xerialHeader[:])
	copy(b[8:], xerialVersionInfo[:])
	binary.BigEndian.PutUint32(b[16:], uint32(n))
}
