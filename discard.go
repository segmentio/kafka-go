package kafka

import (
	"bufio"
	"fmt"
	"reflect"
)

func discardN(r *bufio.Reader, sz int, n int) (int, error) {
	n, err := r.Discard(n)
	return sz - n, err
}

func discardInt8(r *bufio.Reader, sz int) (int, error) {
	return discardN(r, sz, 1)
}

func discardInt16(r *bufio.Reader, sz int) (int, error) {
	return discardN(r, sz, 2)
}

func discardInt32(r *bufio.Reader, sz int) (int, error) {
	return discardN(r, sz, 4)
}

func discardInt64(r *bufio.Reader, sz int) (int, error) {
	return discardN(r, sz, 8)
}

func discardString(r *bufio.Reader, sz int) (int, error) {
	return readStringWith(r, sz, func(r *bufio.Reader, sz int, len int16) (int, error) {
		return discardN(r, sz, int(len))
	})
}

func discardBytes(r *bufio.Reader, sz int) (int, error) {
	return readBytesWith(r, sz, func(r *bufio.Reader, sz int, len int32) (int, error) {
		return discardN(r, sz, int(len))
	})
}

func discard(r *bufio.Reader, sz int, a interface{}) (int, error) {
	switch a.(type) {
	case int8:
		return discardInt8(r, sz)
	case int16:
		return discardInt16(r, sz)
	case int32:
		return discardInt32(r, sz)
	case int64:
		return discardInt64(r, sz)
	case string:
		return discardString(r, sz)
	case []byte:
		return discardBytes(r, sz)
	}
	switch v := reflect.ValueOf(a); v.Kind() {
	case reflect.Struct:
		return discardStruct(r, sz, v)
	case reflect.Slice:
		return discardSlice(r, sz, v)
	default:
		panic(fmt.Sprintf("unsupported type: %T", a))
	}
}

func discardStruct(r *bufio.Reader, sz int, v reflect.Value) (int, error) {
	var err error
	for i, n := 0, v.NumField(); i != n; i++ {
		if sz, err = discard(r, sz, v.Field(i)); err != nil {
			break
		}
	}
	return sz, err
}

func discardSlice(r *bufio.Reader, sz int, v reflect.Value) (int, error) {
	var zero = reflect.Zero(v.Type().Elem())
	var err error
	var len int32

	if sz, err = readInt32(r, sz, &len); err != nil {
		return sz, err
	}

	for n := int(len); n > 0; n-- {
		if sz, err = discard(r, sz, zero); err != nil {
			break
		}
	}

	return sz, err
}
