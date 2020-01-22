package protocol

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
)

type decoder struct {
	reader *bufio.Reader
	remain int
	err    error
	limit  io.LimitedReader
}

func (d *decoder) decodeBool(v value) {
	v.setBool(d.readInt8() != 0)
}

func (d *decoder) decodeInt8(v value) {
	v.setInt8(d.readInt8())
}

func (d *decoder) decodeInt16(v value) {
	v.setInt16(d.readInt16())
}

func (d *decoder) decodeInt32(v value) {
	v.setInt32(d.readInt32())
}

func (d *decoder) decodeInt64(v value) {
	v.setInt64(d.readInt64())
}

func (d *decoder) decodeString(v value) {
	v.setString(d.readString())
}

func (d *decoder) decodeBytes(v value) {
	v.setBytes(d.readBytes())
}

func (d *decoder) decodeArray(v value, elemType reflect.Type, decodeElem decodeFunc) {
	if n := d.readInt32(); n < 0 {
		v.setArray(array{})
	} else {
		a := makeArray(elemType, int(n))
		for i := 0; i < int(n); i++ {
			decodeElem(d, a.index(i))
		}
		v.setArray(a)
	}
}

func (d *decoder) discardAll() {
	if d.remain > 0 {
		d.limit.R = d.reader
		d.limit.N = int64(d.remain)
		n, err := io.Copy(ioutil.Discard, &d.limit)
		d.remain -= int(n)
		d.setError(err)
	}
}

func (d *decoder) discard(n int) {
	d.reader.Discard(n)
	d.remain -= n
}

func (d *decoder) peek(n int) []byte {
	if n > d.remain {
		d.setError(io.ErrUnexpectedEOF)
		return nil
	}
	b, err := d.reader.Peek(n)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		d.setError(err)
		return nil
	}
	return b
}

func (d *decoder) read(n int) []byte {
	if n > d.remain {
		d.setError(io.ErrUnexpectedEOF)
		return nil
	}
	b := make([]byte, n)
	n, err := io.ReadFull(d.reader, b)
	d.remain -= n
	if err != nil {
		b = b[:n]
		d.setError(err)
	}
	return b
}

func (d *decoder) writeTo(n int, w io.Writer) {
	if int(n) > d.remain {
		d.setError(io.ErrUnexpectedEOF)
	} else {
		d.limit.R = d.reader
		d.limit.N = int64(n)
		n, err := io.Copy(w, &d.limit)
		d.remain -= int(n)
		d.setError(err)
	}
}

func (d *decoder) setError(err error) {
	if d.err == nil && err != nil {
		d.err = err
		d.discard(d.remain)
	}
}

func (d *decoder) readInt8() int8 {
	if b := d.peek(1); len(b) == 1 {
		u := b[0]
		d.discard(1)
		return int8(u)
	}
	return 0
}

func (d *decoder) readInt16() int16 {
	if b := d.peek(2); len(b) == 2 {
		i := readInt16(b)
		d.discard(2)
		return i
	}
	return 0
}

func (d *decoder) readInt32() int32 {
	if b := d.peek(4); len(b) == 4 {
		i := readInt32(b)
		d.discard(4)
		return i
	}
	return 0
}

func (d *decoder) readInt64() int64 {
	if b := d.peek(8); len(b) == 8 {
		i := readInt64(b)
		d.discard(8)
		return i
	}
	return 0
}

func (d *decoder) readString() string {
	if n := d.readInt16(); n < 0 {
		return ""
	} else {
		return bytesToString(d.read(int(n)))
	}
}

func (d *decoder) readCompactString() string {
	if n := d.readVarInt(); n < 0 {
		return ""
	} else {
		return bytesToString(d.read(int(n)))
	}
}

func (d *decoder) readBytes() []byte {
	if n := d.readInt32(); n < 0 {
		return nil
	} else {
		return d.read(int(n))
	}
}

func (d *decoder) readBytesTo(w io.Writer) bool {
	if n := d.readInt32(); n < 0 {
		return false
	} else {
		d.writeTo(int(n), w)
		return d.err == nil
	}
}

func (d *decoder) readCompactBytes() []byte {
	if n := d.readVarInt(); n < 0 {
		return nil
	} else {
		return d.read(int(n))
	}
}

func (d *decoder) readCompactBytesTo(w io.Writer) bool {
	if n := d.readVarInt(); n < 0 {
		return false
	} else {
		d.writeTo(int(n), w)
		return d.err == nil
	}
}

func (d *decoder) readVarInt() int64 {
	n := 11 // varints are at most 11 bytes

	if n > d.remain {
		n = d.remain
	}

	if b := d.peek(n); len(b) == n {
		x := uint64(0)
		s := uint(0)

		for i, c := range b {
			if (c & 0x80) == 0 {
				x |= uint64(c) << s
				v := int64(x>>1) ^ -(int64(x) & 1)
				d.discard(i + 1)
				return v
			}
			x |= uint64(c&0x7f) << s
			s += 7
		}

		d.setError(fmt.Errorf("cannot decode varint from %#x", b))
	}

	return 0
}

type decodeFunc func(*decoder, value)

var (
	readerFrom = reflect.TypeOf((*io.ReaderFrom)(nil)).Elem()
)

func decodeFuncOf(typ reflect.Type, version int16, tag structTag) decodeFunc {
	if reflect.PtrTo(typ).Implements(readerFrom) {
		return readerDecodeFuncOf(typ)
	}
	switch typ.Kind() {
	case reflect.Bool:
		return (*decoder).decodeBool
	case reflect.Int8:
		return (*decoder).decodeInt8
	case reflect.Int16:
		return (*decoder).decodeInt16
	case reflect.Int32:
		return (*decoder).decodeInt32
	case reflect.Int64:
		return (*decoder).decodeInt64
	case reflect.String:
		return stringDecodeFuncOf(tag)
	case reflect.Struct:
		return structDecodeFuncOf(typ, version)
	case reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 { // []byte
			return bytesDecodeFuncOf(tag)
		}
		return arrayDecodeFuncOf(typ, version, tag)
	default:
		panic("unsupported type: " + typ.String())
	}
}

func stringDecodeFuncOf(tag structTag) decodeFunc {
	return (*decoder).decodeString
}

func bytesDecodeFuncOf(tag structTag) decodeFunc {
	return (*decoder).decodeBytes
}

func structDecodeFuncOf(typ reflect.Type, version int16) decodeFunc {
	type field struct {
		decode decodeFunc
		index  index
	}

	var fields []field
	forEachStructField(typ, func(typ reflect.Type, index index, tag string) {
		forEachStructTag(tag, func(tag structTag) bool {
			if tag.MinVersion <= version && version <= tag.MaxVersion {
				fields = append(fields, field{
					decode: decodeFuncOf(typ, version, tag),
					index:  index,
				})
				return false
			}
			return true
		})
	})

	return func(d *decoder, v value) {
		for i := range fields {
			f := &fields[i]
			f.decode(d, v.fieldByIndex(f.index))
		}
	}
}

func arrayDecodeFuncOf(typ reflect.Type, version int16, tag structTag) decodeFunc {
	elemType := typ.Elem()
	elemFunc := decodeFuncOf(elemType, version, tag)
	return func(d *decoder, v value) { d.decodeArray(v, elemType, elemFunc) }
}

func readerDecodeFuncOf(typ reflect.Type) decodeFunc {
	typ = reflect.PtrTo(typ)
	return func(d *decoder, v value) {
		if d.err == nil {
			_, d.err = v.iface(typ).(io.ReaderFrom).ReadFrom(d.reader)
		}
	}
}
