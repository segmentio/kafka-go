package kafka

import (
	"fmt"
	"reflect"
)

func sizeof(a interface{}) int32 {
	switch v := a.(type) {
	case int8:
		return 1
	case int16:
		return 2
	case int32:
		return 4
	case int64:
		return 8
	case string:
		return 2 + int32(len(v))
	case []byte:
		return 4 + int32(len(v))
	case messageSet:
		return sizeofMessageSet(v)
	}
	switch v := reflect.ValueOf(a); v.Kind() {
	case reflect.Struct:
		return sizeofStruct(v)
	case reflect.Slice:
		return sizeofSlice(v)
	default:
		panic(fmt.Sprintf("unsupported type: %T", a))
	}
}

func sizeofMessageSet(set messageSet) (size int32) {
	for _, msg := range set {
		size += sizeof(msg)
	}
	return
}

func sizeofStruct(v reflect.Value) (size int32) {
	for i, n := 0, v.NumField(); i != n; i++ {
		size += sizeof(v.Field(i).Interface())
	}
	return
}

func sizeofSlice(v reflect.Value) (size int32) {
	size = 4
	for i, n := 0, v.Len(); i != n; i++ {
		size += sizeof(v.Index(i).Interface())
	}
	return
}
