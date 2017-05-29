package kafka

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"reflect"
)

func writeInt8(w *bufio.Writer, i int8) error {
	return w.WriteByte(byte(i))
}

func writeInt16(w *bufio.Writer, i int16) error {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(i))
	return writeSmallBuffer(w, b[:])
}

func writeInt32(w *bufio.Writer, i int32) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(i))
	return writeSmallBuffer(w, b[:])
}

func writeInt64(w *bufio.Writer, i int64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(i))
	return writeSmallBuffer(w, b[:])
}

func writeString(w *bufio.Writer, s string) error {
	if err := writeInt16(w, int16(len(s))); err != nil {
		return err
	}
	_, err := w.WriteString(s)
	return err
}

func writeBytes(w *bufio.Writer, b []byte) error {
	n := int32(len(b))
	if b == nil {
		n = -1
	}
	if err := writeInt32(w, n); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func writeMessageSet(w *bufio.Writer, mset messageSet) error {
	// message sets are not preceded by their size for some reason
	for _, m := range mset {
		if err := write(w, m); err != nil {
			return err
		}
	}
	return nil
}

func writeSmallBuffer(w *bufio.Writer, b []byte) error {
	// faster for short byte slices because it doesn't cause a memory allocation
	// because the byte slice may be passed to the underlying io.Writer.
	for _, c := range b {
		if err := w.WriteByte(c); err != nil {
			return err
		}
	}
	return nil
}

func write(w *bufio.Writer, a interface{}) error {
	switch v := a.(type) {
	case int8:
		return writeInt8(w, v)
	case int16:
		return writeInt16(w, v)
	case int32:
		return writeInt32(w, v)
	case int64:
		return writeInt64(w, v)
	case string:
		return writeString(w, v)
	case []byte:
		return writeBytes(w, v)
	case messageSet:
		return writeMessageSet(w, v)
	}
	switch v := reflect.ValueOf(a); v.Kind() {
	case reflect.Struct:
		return writeStruct(w, v)
	case reflect.Slice:
		return writeSlice(w, v)
	default:
		panic(fmt.Sprintf("unsupported type: %T", a))
	}
}

func writeStruct(w *bufio.Writer, v reflect.Value) error {
	for i, n := 0, v.NumField(); i != n; i++ {
		if err := write(w, v.Field(i).Interface()); err != nil {
			return err
		}
	}
	return nil
}

func writeSlice(w *bufio.Writer, v reflect.Value) error {
	n := v.Len()
	if err := writeInt32(w, int32(n)); err != nil {
		return err
	}
	for i := 0; i != n; i++ {
		if err := write(w, v.Index(i).Interface()); err != nil {
			return err
		}
	}
	return nil
}

func writeRequest(w *bufio.Writer, hdr requestHeader, req interface{}) error {
	hdr.Size = (sizeof(hdr) + sizeof(req)) - 4
	write(w, hdr)
	write(w, req)
	return w.Flush()
}
