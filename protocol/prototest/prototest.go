package prototest

import (
	"bytes"
	"io"
	"reflect"
	"time"

	"github.com/apoorvag-mav/kafka-go/protocol"
)

func deepEqual(x1, x2 interface{}) bool {
	if x1 == nil {
		return x2 == nil
	}
	if r1, ok := x1.(protocol.RecordReader); ok {
		if r2, ok := x2.(protocol.RecordReader); ok {
			return deepEqualRecords(r1, r2)
		}
		return false
	}
	if b1, ok := x1.(protocol.Bytes); ok {
		if b2, ok := x2.(protocol.Bytes); ok {
			return deepEqualBytes(b1, b2)
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

func deepEqualBytes(s1, s2 protocol.Bytes) bool {
	if s1 == nil {
		return s2 == nil
	}

	if s2 == nil {
		return false
	}

	n1 := s1.Len()
	n2 := s2.Len()

	if n1 != n2 {
		return false
	}

	b1 := make([]byte, n1)
	b2 := make([]byte, n2)

	if _, err := s1.(io.ReaderAt).ReadAt(b1, 0); err != nil {
		panic(err)
	}

	if _, err := s2.(io.ReaderAt).ReadAt(b2, 0); err != nil {
		panic(err)
	}

	return bytes.Equal(b1, b2)
}

func deepEqualRecords(r1, r2 protocol.RecordReader) bool {
	for {
		rec1, err1 := r1.ReadRecord()
		rec2, err2 := r2.ReadRecord()

		if err1 != nil || err2 != nil {
			return err1 == err2
		}

		if !deepEqualRecord(rec1, rec2) {
			return false
		}
	}
}

func deepEqualRecord(r1, r2 *protocol.Record) bool {
	if r1.Offset != r2.Offset {
		return false
	}

	if !r1.Time.Equal(r2.Time) {
		return false
	}

	if !deepEqualBytes(r1.Key, r2.Key) {
		return false
	}

	if !deepEqualBytes(r1.Value, r2.Value) {
		return false
	}

	return deepEqual(r1.Headers, r2.Headers)
}

func reset(v interface{}) {
	if r, _ := v.(interface{ Reset() }); r != nil {
		r.Reset()
	}
}
