package protocol

import (
	"reflect"
)

func sizeOf(typ reflect.Type) int {
	switch typ.Kind() {
	case reflect.Bool, reflect.Int8:
		return 1
	case reflect.Int16:
		return 2
	case reflect.Int32:
		return 4
	case reflect.Int64:
		return 8
	default:
		return 0
	}
}

func sizeOfString(s string) int {
	return 2 + len(s)
}

func sizeOfBytes(b []byte) int {
	return 4 + len(b)
}

func sizeOfCompactString(s string) int {
	return sizeOfVarInt(int64(len(s))) + len(s)
}

func sizeOfCompactBytes(b []byte) int {
	return sizeOfVarInt(int64(len(b))) + len(b)
}

func sizeOfCompactNullString(s string) int {
	n := len(s)
	if n == 0 {
		n = -1
	}
	return sizeOfVarInt(int64(n)) + len(s)
}

func sizeOfCompactNullBytes(b []byte) int {
	n := len(b)
	if b == nil {
		n = -1
	}
	return sizeOfVarInt(int64(n)) + len(b)
}

func sizeOfVarInt(i int64) int {
	u := uint64((i << 1) ^ (i >> 63)) // zig-zag encoding
	n := 0

	for u >= 0x80 {
		u >>= 7
		n++
	}

	return n + 1
}

func sizeOfVarBytes(b Bytes) int {
	if b == nil {
		return sizeOfVarInt(-1)
	} else {
		n := b.Len()
		return sizeOfVarInt(int64(n)) + n
	}
}
