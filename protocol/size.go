package protocol

import (
	"reflect"
)

type sizeFunc func(value) int

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

func sizeFuncOf(typ reflect.Type, version int16, tag structTag) sizeFunc {
	switch typ.Kind() {
	case reflect.String:
		return stringSizeFuncOf(tag)
	case reflect.Struct:
		return structSizeFuncOf(typ, version)
	case reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 { // []byte
			return bytesSizeFuncOf(tag)
		}
		return arraySizeFuncOf(typ, version, tag)
	default:
		panic("unsupported type: " + typ.String())
	}
}

func stringSizeFuncOf(tag structTag) sizeFunc {
	return func(v value) int { return sizeOfString(v.string()) }
}

func bytesSizeFuncOf(tag structTag) sizeFunc {
	return func(v value) int { return sizeOfBytes(v.bytes()) }
}

func structSizeFuncOf(typ reflect.Type, version int16) sizeFunc {
	type field struct {
		size  sizeFunc
		index index
	}

	var fields []field
	var fixedSize int
	forEachStructField(typ, func(typ reflect.Type, index index, tag string) {
		forEachStructTag(tag, func(tag structTag) bool {
			if tag.MinVersion <= version && version <= tag.MaxVersion {
				if size := sizeOf(typ); size != 0 {
					fixedSize += size
				} else {
					fields = append(fields, field{
						size:  sizeFuncOf(typ, version, tag),
						index: index,
					})
				}
				return false
			}
			return true
		})
	})

	return func(v value) int {
		size := fixedSize

		for i := range fields {
			f := &fields[i]
			size += f.size(v.fieldByIndex(f.index))
		}

		return size
	}
}

func arraySizeFuncOf(typ reflect.Type, version int16, tag structTag) sizeFunc {
	elemType := typ.Elem()
	elemSize := sizeOf(elemType)
	if elemSize != 0 {
		// array of fixed-size elements
		return func(v value) int { return 4 + v.array(elemType).length()*elemSize }
	}
	elemFunc := sizeFuncOf(elemType, version, tag)
	return func(v value) int {
		size := 4
		a := v.array(elemType)
		n := a.length()

		for i := 0; i < n; i++ {
			size += elemFunc(a.index(i))
		}

		return size
	}
}
