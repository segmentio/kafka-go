package types

import (
	"fmt"
	"io"
	"reflect"
)

func Marshal(w io.Writer, v interface{}) error {
	x, _ := w.(*Writer)
	if x == nil {
		x = NewWriter(w)
	}
	marshal(x, reflect.ValueOf(v).Elem())
	return x.Err()
}

func marshal(w *Writer, v reflect.Value) {
	switch v.Kind() {
	case reflect.Bool:
		marshalBool(w, v)
	case reflect.Uint8:
		marshalUint8(w, v)
	case reflect.Uint16:
		marshalUint16(w, v)
	case reflect.Uint32:
		marshalUint32(w, v)
	case reflect.Uint64:
		marshalUint64(w, v)
	case reflect.Int8:
		marshalInt8(w, v)
	case reflect.Int16:
		marshalInt16(w, v)
	case reflect.Int32:
		marshalInt32(w, v)
	case reflect.Int64:
		marshalInt64(w, v)
	case reflect.String:
		marshalString(w, v)
	case reflect.Struct:
		marshalStruct(w, v)
	case reflect.Slice:
		marshalSlice(w, v)
	case reflect.Map:
		marshalMap(w, v)
	default:
		panic(fmt.Errorf("unsupported type: %s", v.Type()))
	}
}

func marshalBool(w *Writer, v reflect.Value) {
	w.WriteBool(v.Bool())
}

func marshalUint8(w *Writer, v reflect.Value) {
	w.WriteUint8(uint8(v.Uint()))
}

func marshalUint16(w *Writer, v reflect.Value) {
	w.WriteUint16(uint16(v.Uint()))
}

func marshalUint32(w *Writer, v reflect.Value) {
	w.WriteUint32(uint32(v.Uint()))
}

func marshalUint64(w *Writer, v reflect.Value) {
	w.WriteUint64(v.Uint())
}

func marshalInt8(w *Writer, v reflect.Value) {
	w.WriteInt8(int8(v.Int()))
}

func marshalInt16(w *Writer, v reflect.Value) {
	w.WriteInt16(int16(v.Int()))
}

func marshalInt32(w *Writer, v reflect.Value) {
	switch v.Type() {
	case varIntType:
		w.WriteVarInt(int32(v.Int()))
	default:
		w.WriteInt32(int32(v.Int()))
	}
}

func marshalInt64(w *Writer, v reflect.Value) {
	switch v.Type() {
	case varLongType:
		w.WriteVarLong(v.Int())
	default:
		w.WriteInt64(v.Int())
	}
}

func marshalString(w *Writer, v reflect.Value) {
	switch v.Type() {
	case varStringType:
		w.WriteVarString(v.String())
	case nullStringType:
		w.WriteNullableString(v.String())
	default:
		w.WriteFixString(v.String())
	}
}

func marshalBytes(w *Writer, v reflect.Value) {
	switch v.Type() {
	case varBytesType:
		w.WriteVarBytes(v.Bytes())
	case nullBytesType:
		w.WriteNullableBytes(v.Bytes())
	default:
		w.WriteFixBytes(v.Bytes())
	}
}

func marshalStruct(w *Writer, v reflect.Value) {
	for i, n := 0, v.NumField(); i < n; i++ {
		marshal(w, v.Field(i))
	}
}

func marshalSlice(w *Writer, v reflect.Value) {
	t := v.Type()
	e := t.Elem()

	switch e.Kind() {
	case reflect.Uint8: // []byte
		marshalBytes(w, v)
	default:
		n := v.Len()
		w.WriteArrayLength(n)

		for i := 0; i < n; i++ {
			marshal(w, v.Index(i))
		}
	}
}

func marshalMap(w *Writer, v reflect.Value) {
	n := v.Len()
	w.WriteInt32(int32(n))

	it := v.MapRange()
	for it.Next() {
		marshal(w, it.Key())
		marshal(w, it.Value())
	}
}
