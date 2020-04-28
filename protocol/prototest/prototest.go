package prototest

import (
	"bytes"
	"io"
	"reflect"
	"time"

	"github.com/segmentio/kafka-go/protocol"
)

func closeMessage(m protocol.Message) {
	if c, ok := m.(io.Closer); ok {
		c.Close()
	}
}

func deepEqual(x1, x2 interface{}) bool {
	if x1 == nil {
		return x2 == nil
	}
	if b1, ok := x1.(protocol.ByteSequence); ok {
		if b2, ok := x2.(protocol.ByteSequence); ok {
			return deepEqualByteSequence(b1, b2)
		}
		return false
	}
	if t1, ok := x1.(time.Time); ok {
		if t2, ok := x2.(time.Time); ok {
			return t1.Equal(t2)
		}
		return false
	}
	return deepEqualValue(reflect.ValueOf(x1), reflect.ValueOf(x2))
}

func deepEqualValue(v1, v2 reflect.Value) bool {
	t1 := v1.Type()
	t2 := v2.Type()

	if t1 != t2 {
		return false
	}

	switch v1.Kind() {
	case reflect.Bool:
		return v1.Bool() == v2.Bool()
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v1.Int() == v2.Int()
	case reflect.String:
		return v1.String() == v2.String()
	case reflect.Struct:
		return deepEqualStruct(v1, v2)
	case reflect.Ptr:
		return deepEqualPtr(v1, v2)
	case reflect.Slice:
		return deepEqualSlice(v1, v2)
	default:
		panic("comparing values of unsupported type: " + v1.Type().String())
	}
}

func deepEqualPtr(v1, v2 reflect.Value) bool {
	if v1.IsNil() {
		return v2.IsNil()
	}
	return deepEqual(v1.Elem().Interface(), v2.Elem().Interface())
}

func deepEqualStruct(v1, v2 reflect.Value) bool {
	t := v1.Type()
	n := t.NumField()

	for i := 0; i < n; i++ {
		f := t.Field(i)

		if f.PkgPath != "" { // ignore unexported fields
			continue
		}

		f1 := v1.Field(i)
		f2 := v2.Field(i)

		if !deepEqual(f1.Interface(), f2.Interface()) {
			return false
		}
	}

	return true
}

func deepEqualSlice(v1, v2 reflect.Value) bool {
	t := v1.Type()
	e := t.Elem()

	if e.Kind() == reflect.Uint8 { // []byte
		return bytes.Equal(v1.Bytes(), v2.Bytes())
	}

	n1 := v1.Len()
	n2 := v2.Len()

	if n1 != n2 {
		return false
	}

	for i := 0; i < n1; i++ {
		f1 := v1.Index(i)
		f2 := v2.Index(i)

		if !deepEqual(f1.Interface(), f2.Interface()) {
			return false
		}
	}

	return true
}

func deepEqualByteSequence(s1, s2 protocol.ByteSequence) bool {
	n1 := s1.Size()
	n2 := s2.Size()

	if n1 != n2 {
		return false
	}

	b1 := make([]byte, n1)
	b2 := make([]byte, n2)

	if _, err := s1.ReadAt(b1, 0); err != nil {
		panic(err)
	}

	if _, err := s2.ReadAt(b2, 0); err != nil {
		panic(err)
	}

	return bytes.Equal(b1, b2)
}
