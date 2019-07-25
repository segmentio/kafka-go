package types

import (
	"fmt"
	"io"
	"math"
	"reflect"
)

func Unmarshal(r io.Reader, v interface{}) error {
	x, _ := r.(*Reader)
	if x == nil {
		x = NewReader(r, math.MaxInt64)
	}
	unmarshal(x, reflect.ValueOf(v).Elem())
	return x.Err()
}

func unmarshal(r *Reader, v reflect.Value) {
	switch v.Kind() {
	case reflect.Bool:
		unmarshalBool(r, v)
	case reflect.Uint8:
		unmarshalUint8(r, v)
	case reflect.Uint16:
		unmarshalUint16(r, v)
	case reflect.Uint32:
		unmarshalUint32(r, v)
	case reflect.Uint64:
		unmarshalUint64(r, v)
	case reflect.Int8:
		unmarshalInt8(r, v)
	case reflect.Int16:
		unmarshalInt16(r, v)
	case reflect.Int32:
		unmarshalInt32(r, v)
	case reflect.Int64:
		unmarshalInt64(r, v)
	case reflect.String:
		unmarshalString(r, v)
	case reflect.Struct:
		unmarshalStruct(r, v)
	case reflect.Slice:
		unmarshalSlice(r, v)
	case reflect.Map:
		unmarshalMap(r, v)
	default:
		panic(fmt.Errorf("unsupported type: %s", v.Type()))
	}
}

func unmarshalBool(r *Reader, v reflect.Value) {
	v.SetBool(r.ReadBool())
}

func unmarshalUint8(r *Reader, v reflect.Value) {
	v.SetUint(uint64(r.ReadUint8()))
}

func unmarshalUint16(r *Reader, v reflect.Value) {
	v.SetUint(uint64(r.ReadUint16()))
}

func unmarshalUint32(r *Reader, v reflect.Value) {
	v.SetUint(uint64(r.ReadUint32()))
}

func unmarshalUint64(r *Reader, v reflect.Value) {
	v.SetUint(uint64(r.ReadUint64()))
}

func unmarshalInt8(r *Reader, v reflect.Value) {
	v.SetInt(int64(r.ReadInt8()))
}

func unmarshalInt16(r *Reader, v reflect.Value) {
	v.SetInt(int64(r.ReadInt16()))
}

func unmarshalInt32(r *Reader, v reflect.Value) {
	switch v.Type() {
	case varIntType:
		v.SetInt(int64(r.ReadVarInt()))
	default:
		v.SetInt(int64(r.ReadInt32()))
	}
}

func unmarshalInt64(r *Reader, v reflect.Value) {
	switch v.Type() {
	case varLongType:
		v.SetInt(int64(r.ReadVarLong()))
	default:
		v.SetInt(int64(r.ReadInt64()))
	}
}

func unmarshalString(r *Reader, v reflect.Value) {
	switch v.Type() {
	case varStringType:
		v.SetString(r.ReadVarString())
	default:
		v.SetString(r.ReadFixString())
	}
}

func unmarshalBytes(r *Reader, v reflect.Value) {
	switch v.Type() {
	case varBytesType:
		v.SetBytes(r.ReadVarBytes())
	default:
		v.SetBytes(r.ReadFixBytes())
	}
}

func unmarshalStruct(r *Reader, v reflect.Value) {
	for i, n := 0, v.NumField(); i < n; i++ {
		unmarshal(r, v.Field(i))
	}
}

func unmarshalSlice(r *Reader, v reflect.Value) {
	t := v.Type()
	e := t.Elem()

	switch e.Kind() {
	case reflect.Uint8: // []byte
		unmarshalBytes(r, v)
	default:
		n := r.ReadArrayLength()

		if n <= 0 {
			v.Set(reflect.Zero(t))
		} else {
			v.Set(reflect.MakeSlice(t, n, n))

			for i := 0; i < n; i++ {
				unmarshal(r, v.Index(i))
			}
		}
	}
}

func unmarshalMap(r *Reader, v reflect.Value) {
	t := v.Type()

	if v.IsNil() {
		v.Set(reflect.New(t).Elem())
	}

	kt := t.Key()
	kv := reflect.New(kt).Elem()
	kz := reflect.Zero(kt)

	vt := t.Elem()
	vv := reflect.New(vt).Elem()
	vz := reflect.Zero(vt)

	n := int(r.ReadInt32())

	for i := 0; i < n; i++ {
		unmarshal(r, kv)
		unmarshal(r, vv)

		v.SetMapIndex(kv, vv)

		kv.Set(kz)
		vv.Set(vz)
	}
}
