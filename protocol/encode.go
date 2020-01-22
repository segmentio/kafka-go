package protocol

import (
	"encoding/binary"
	"io"
	"reflect"
)

type encoder struct {
	writer io.Writer
	buffer [16]byte
}

func (e *encoder) encodeBool(v value) {
	b := int8(0)
	if v.bool() {
		b = 1
	}
	e.writeInt8(b)
}

func (e *encoder) encodeInt8(v value) {
	e.writeInt8(v.int8())
}

func (e *encoder) encodeInt16(v value) {
	e.writeInt16(v.int16())
}

func (e *encoder) encodeInt32(v value) {
	e.writeInt32(v.int32())
}

func (e *encoder) encodeInt64(v value) {
	e.writeInt64(v.int64())
}

func (e *encoder) encodeString(v value) {
	e.writeString(v.string())
}

func (e *encoder) encodeNullString(v value) {
	e.writeNullString(v.string())
}

func (e *encoder) encodeBytes(v value) {
	e.writeBytes(v.bytes())
}

func (e *encoder) encodeNullBytes(v value) {
	e.writeNullBytes(v.bytes())
}

func (e *encoder) encodeArray(v value, elemType reflect.Type, encodeElem encodeFunc) {
	a := v.array(elemType)
	n := a.length()
	e.writeInt32(int32(n))

	for i := 0; i < n; i++ {
		encodeElem(e, a.index(i))
	}
}

func (e *encoder) encodeNullArray(v value, elemType reflect.Type, encodeElem encodeFunc) {
	a := v.array(elemType)
	if a.isNil() {
		e.writeInt32(-1)
		return
	}

	n := a.length()
	e.writeInt32(int32(n))

	for i := 0; i < n; i++ {
		encodeElem(e, a.index(i))
	}
}

func (e *encoder) writeInt8(i int8) {
	e.buffer[0] = byte(i)
	e.writer.Write(e.buffer[:1])
}

func (e *encoder) writeInt16(i int16) {
	binary.LittleEndian.PutUint16(e.buffer[:2], uint16(i))
	e.writer.Write(e.buffer[:2])
}

func (e *encoder) writeInt32(i int32) {
	binary.LittleEndian.PutUint32(e.buffer[:4], uint32(i))
	e.writer.Write(e.buffer[:4])
}

func (e *encoder) writeInt64(i int64) {
	binary.LittleEndian.PutUint64(e.buffer[:8], uint64(i))
	e.writer.Write(e.buffer[:8])
}

func (e *encoder) writeString(s string) {
	e.writeInt16(int16(len(s)))
	io.WriteString(e.writer, s)
}

func (e *encoder) writeNullString(s string) {
	if s == "" {
		e.writeInt16(-1)
	} else {
		e.writeInt16(int16(len(s)))
		io.WriteString(e.writer, s)
	}
}

func (e *encoder) writeBytes(b []byte) {
	e.writeInt32(int32(len(b)))
	e.writer.Write(b)
}

func (e *encoder) writeNullBytes(b []byte) {
	if b == nil {
		e.writeInt32(-1)
	} else {
		e.writeInt32(int32(len(b)))
		e.writer.Write(b)
	}
}

func (e *encoder) writeBytesFrom(b ByteSequence) error {
	e.writeInt32(int32(b.Size()))
	_, err := copyBytes(e.writer, b)
	return err
}

func (e *encoder) writeNullBytesFrom(b ByteSequence) error {
	if b == nil {
		e.writeInt32(-1)
		return nil
	} else {
		e.writeInt32(int32(b.Size()))
		_, err := copyBytes(e.writer, b)
		return err
	}
}

type encodeFunc func(*encoder, value)

var (
	writerTo = reflect.TypeOf((*io.WriterTo)(nil)).Elem()
)

func encodeFuncOf(typ reflect.Type, version int16, tag structTag) encodeFunc {
	if reflect.PtrTo(typ).Implements(writerTo) {
		return writerEncodeFuncOf(typ)
	}
	switch typ.Kind() {
	case reflect.Bool:
		return (*encoder).encodeBool
	case reflect.Int8:
		return (*encoder).encodeInt8
	case reflect.Int16:
		return (*encoder).encodeInt16
	case reflect.Int32:
		return (*encoder).encodeInt32
	case reflect.Int64:
		return (*encoder).encodeInt64
	case reflect.String:
		return stringEncodeFuncOf(tag)
	case reflect.Struct:
		return structEncodeFuncOf(typ, version)
	case reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 { // []byte
			return bytesEncodeFuncOf(tag)
		}
		return arrayEncodeFuncOf(typ, version, tag)
	default:
		panic("unsupported type: " + typ.String())
	}
}

func stringEncodeFuncOf(tag structTag) encodeFunc {
	switch {
	case tag.Nullable:
		return (*encoder).encodeNullString
	default:
		return (*encoder).encodeString
	}
}

func bytesEncodeFuncOf(tag structTag) encodeFunc {
	switch {
	case tag.Nullable:
		return (*encoder).encodeNullBytes
	default:
		return (*encoder).encodeBytes
	}
}

func structEncodeFuncOf(typ reflect.Type, version int16) encodeFunc {
	type field struct {
		encode encodeFunc
		index  index
	}

	var fields []field
	forEachStructField(typ, func(typ reflect.Type, index index, tag string) {
		forEachStructTag(tag, func(tag structTag) bool {
			if tag.MinVersion <= version && version <= tag.MaxVersion {
				fields = append(fields, field{
					encode: encodeFuncOf(typ, version, tag),
					index:  index,
				})
				return false
			}
			return true
		})
	})

	return func(e *encoder, v value) {
		for i := range fields {
			f := &fields[i]
			f.encode(e, v.fieldByIndex(f.index))
		}
	}
}

func arrayEncodeFuncOf(typ reflect.Type, version int16, tag structTag) encodeFunc {
	elemType := typ.Elem()
	elemFunc := encodeFuncOf(elemType, version, tag)
	switch {
	case tag.Nullable:
		return func(e *encoder, v value) { e.encodeNullArray(v, elemType, elemFunc) }
	default:
		return func(e *encoder, v value) { e.encodeArray(v, elemType, elemFunc) }
	}
}

func writerEncodeFuncOf(typ reflect.Type) encodeFunc {
	typ = reflect.PtrTo(typ)
	return func(e *encoder, v value) { v.iface(typ).(io.WriterTo).WriteTo(e.writer) }
}
