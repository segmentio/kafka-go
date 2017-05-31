package kafka

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"reflect"
)

func writeInt8(w *bufio.Writer, i int8) {
	w.WriteByte(byte(i))
}

func writeInt16(w *bufio.Writer, i int16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(i))
	w.WriteByte(b[0])
	w.WriteByte(b[1])
}

func writeInt32(w *bufio.Writer, i int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(i))
	w.WriteByte(b[0])
	w.WriteByte(b[1])
	w.WriteByte(b[2])
	w.WriteByte(b[3])
}

func writeInt64(w *bufio.Writer, i int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(i))
	w.WriteByte(b[0])
	w.WriteByte(b[1])
	w.WriteByte(b[2])
	w.WriteByte(b[3])
	w.WriteByte(b[4])
	w.WriteByte(b[5])
	w.WriteByte(b[6])
	w.WriteByte(b[7])
}

func writeString(w *bufio.Writer, s string) {
	writeInt16(w, int16(len(s)))
	w.WriteString(s)
}

func writeBytes(w *bufio.Writer, b []byte) {
	n := len(b)
	if b == nil {
		n = -1
	}
	writeInt32(w, int32(n))
	w.Write(b)
}

func writeMessageSet(w *bufio.Writer, mset messageSet) {
	for _, m := range mset {
		write(w, m)
	}
}

func write(w *bufio.Writer, a interface{}) {
	switch v := a.(type) {
	case int8:
		writeInt8(w, v)
	case int16:
		writeInt16(w, v)
	case int32:
		writeInt32(w, v)
	case int64:
		writeInt64(w, v)
	case string:
		writeString(w, v)
	case []byte:
		writeBytes(w, v)
	case messageSet:
		writeMessageSet(w, v)
	default:
		switch v := reflect.ValueOf(a); v.Kind() {
		case reflect.Struct:
			writeStruct(w, v)
		case reflect.Slice:
			writeSlice(w, v)
		default:
			panic(fmt.Sprintf("unsupported type: %T", a))
		}
	}
}

func writeStruct(w *bufio.Writer, v reflect.Value) {
	for i, n := 0, v.NumField(); i != n; i++ {
		write(w, v.Field(i).Interface())
	}
}

func writeSlice(w *bufio.Writer, v reflect.Value) {
	n := v.Len()
	writeInt32(w, int32(n))
	for i := 0; i != n; i++ {
		write(w, v.Index(i).Interface())
	}
}

func writeRequest(w *bufio.Writer, hdr requestHeader, req interface{}) error {
	hdr.Size = (sizeof(hdr) + sizeof(req)) - 4
	write(w, hdr)
	write(w, req)
	return w.Flush()
}
