package protocol

import (
	"bufio"
	"encoding/binary"
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
		if err != nil {
			d.err = err
		}
		d.remain -= int(n)
	}
}

func (d *decoder) discard(n int) {
	d.reader.Discard(n)
	d.remain -= n
}

func (d *decoder) peek(n int) []byte {
	if n > d.remain {
		d.err = ErrTruncated
		return nil
	}
	b, err := d.reader.Peek(n)
	if err != nil && d.err == nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		d.err = err
		return nil
	}
	return b
}

func (d *decoder) read(n int) []byte {
	if n > d.remain {
		d.err = ErrTruncated
		return nil
	}
	b := make([]byte, n)
	n, err := io.ReadFull(d.reader, b)
	if err != nil {
		b = b[:n]
		if d.err == nil {
			d.err = err
		}
	}
	d.remain -= n
	return b
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
		u := binary.LittleEndian.Uint16(b)
		d.discard(2)
		return int16(u)
	}
	return 0
}

func (d *decoder) readInt32() int32 {
	if b := d.peek(4); len(b) == 4 {
		u := binary.LittleEndian.Uint32(b)
		d.discard(4)
		return int32(u)
	}
	return 0
}

func (d *decoder) readInt64() int64 {
	if b := d.peek(8); len(b) == 8 {
		u := binary.LittleEndian.Uint64(b)
		d.discard(8)
		return int64(u)
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
		if int(n) > d.remain {
			d.err = ErrTruncated
			return false
		}
		d.limit.R = d.reader
		d.limit.N = int64(n)
		n, err := io.Copy(w, &d.limit)
		d.remain -= int(n)
		if err != nil {
			d.err = err
		}
		return true
	}
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
