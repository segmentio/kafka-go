package types

import (
	"io"
)

type Writer struct {
	w   io.Writer
	b   [16]byte
	err error
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

func (w *Writer) Err() error {
	return w.err
}

func (w *Writer) Flush() error {
	if err := w.err; err != nil {
		return err
	}
	if f, _ := w.w.(interface{ Flush() error }); f != nil {
		if err := f.Flush(); err != nil {
			w.err = err
			return err
		}
	}
	return nil
}

func (w *Writer) Reset(x io.Writer) {
	w.w = x
	w.b = [16]byte{}
	w.err = nil
}

func (w *Writer) WriteUint8(i uint8) {
	w.b[0] = byte(i)
	w.Write(w.b[:1])
}

func (w *Writer) WriteUint16(i uint16) {
	putUint16(w.b[:2], i)
	w.Write(w.b[:2])
}

func (w *Writer) WriteUint32(i uint32) {
	putUint32(w.b[:4], i)
	w.Write(w.b[:4])
}

func (w *Writer) WriteUint64(i uint64) {
	putUint64(w.b[:8], i)
	w.Write(w.b[:8])
}

func (w *Writer) WriteInt8(i int8) {
	w.WriteUint8(uint8(i))
}

func (w *Writer) WriteInt16(i int16) {
	w.WriteUint16(uint16(i))
}

func (w *Writer) WriteInt32(i int32) {
	w.WriteUint32(uint32(i))
}

func (w *Writer) WriteInt64(i int64) {
	w.WriteUint64(uint64(i))
}

func (w *Writer) WriteVarInt(i int32) {
	w.WriteVarLong(int64(i))
}

func (w *Writer) WriteVarLong(i int64) {
	u := uint64((i << 1) ^ (i >> 63))
	n := 0

	for u >= 0x80 && n < len(w.b) {
		w.b[n] = byte(u) | 0x80
		u >>= 7
		n++
	}

	if n < len(w.b) {
		w.b[n] = byte(u)
		n++
	}

	w.Write(w.b[:n])
}

func (w *Writer) WriteFixString(s string) {
	w.WriteInt16(int16(len(s)))
	w.WriteString(s)
}

func (w *Writer) WriteVarString(s string) {
	w.WriteVarInt(int32(len(s)))
	w.WriteString(s)
}

func (w *Writer) WriteNullableString(s string) {
	if s == "" {
		w.WriteInt16(-1)
	} else {
		w.WriteFixString(s)
	}
}

func (w *Writer) WriteFixBytes(b []byte) {
	w.WriteInt32(int32(len(b)))
	w.Write(b)
}

func (w *Writer) WriteVarBytes(b []byte) {
	w.WriteVarInt(int32(len(b)))
	w.Write(b)
}

func (w *Writer) WriteNullableBytes(b []byte) {
	if b == nil {
		w.WriteInt32(-1)
	} else {
		w.WriteFixBytes(b)
	}
}

func (w *Writer) WriteBool(b bool) {
	v := uint8(0)
	if b {
		v = 1
	}
	w.WriteUint8(v)
}

func (w *Writer) WriteArrayLength(n int) {
	w.WriteInt32(int32(n))
}

func (w *Writer) Write(b []byte) (int, error) {
	if err := w.err; err != nil {
		return 0, err
	}
	n, err := w.w.Write(b)
	if err != nil {
		w.err = err
	}
	return n, err
}

func (w *Writer) WriteByte(b byte) error {
	w.b[0] = b
	_, err := w.Write(w.b[:1])
	return err
}

func (w *Writer) WriteString(s string) (int, error) {
	if err := w.err; err != nil {
		return 0, err
	}
	n, err := io.WriteString(w.w, s)
	if err != nil {
		w.err = err
	}
	return n, err
}
