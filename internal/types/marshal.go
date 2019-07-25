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

func marshalInt8(w *Writer, v reflect.Value) {
	w.WriteInt8(int8(v.Int()))
}

func marshalInt16(w *Writer, v reflect.Value) {
	w.WriteInt16(int16(v.Int()))
}

func marshalInt32(w *Writer, v reflect.Value) {
	w.WriteInt32(int32(v.Int()))
}

func marshalInt64(w *Writer, v reflect.Value) {
	if v.Type() == varIntType {
		w.WriteVarInt(v.Int())
	} else {
		w.WriteInt64(v.Int())
	}
}

func marshalString(w *Writer, v reflect.Value) {
	if v.Type() == varStringType {
		w.WriteVarString(v.String())
	} else {
		w.WriteFixString(v.String())
	}
}

func marshalBytes(w *Writer, v reflect.Value) {
	if v.Type() == varBytesType {
		w.WriteVarBytes(v.Bytes())
	} else {
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
		w.WriteInt32(int32(n))

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
