package types

import (
	"errors"
	"reflect"
)

var ErrShortRead = errors.New("not enough bytes available to load the response")

type VarInt int64

type VarString string

type VarBytes []byte

var (
	varIntType    = reflect.TypeOf(VarInt(0))
	varStringType = reflect.TypeOf(VarString(""))
	varBytesType  = reflect.TypeOf(VarBytes(nil))
)

func makeInt16(b []byte) int16 {
	_ = b[1]
	return int16(b[1])<<8 | int16(b[0])
}

func makeInt32(b []byte) int32 {
	_ = b[3]
	return int32(b[3])<<24 |
		int32(b[2])<<16 |
		int32(b[1])<<8 |
		int32(b[0])
}

func makeInt64(b []byte) int64 {
	_ = b[7]
	return int64(b[7])<<56 |
		int64(b[6])<<48 |
		int64(b[5])<<40 |
		int64(b[4])<<32 |
		int64(b[3])<<24 |
		int64(b[2])<<16 |
		int64(b[1])<<8 |
		int64(b[0])
}

func putInt16(b []byte, i int16) {
	_ = b[1]
	b[0] = byte(i)
	b[1] = byte(i >> 8)
}

func putInt32(b []byte, i int32) {
	_ = b[3]
	b[0] = byte(i)
	b[1] = byte(i >> 8)
	b[2] = byte(i >> 16)
	b[3] = byte(i >> 24)
}

func putInt64(b []byte, i int64) {
	_ = b[7]
	b[0] = byte(i)
	b[1] = byte(i >> 8)
	b[2] = byte(i >> 16)
	b[3] = byte(i >> 24)
	b[4] = byte(i >> 32)
	b[5] = byte(i >> 40)
	b[6] = byte(i >> 48)
	b[7] = byte(i >> 56)
}
