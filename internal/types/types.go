package types

import (
	"errors"
	"reflect"
)

type VarInt int32

type VarLong int64

type VarString string

type NullableString string

type VarBytes []byte

type NullableBytes []byte

type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      NullableString
}

var (
	ErrShortRead = errors.New("not enough bytes available to load the response")

	ErrVarIntRange = errors.New("varint value out of range")

	varIntType     = reflect.TypeOf(VarInt(0))
	varLongType    = reflect.TypeOf(VarLong(0))
	varStringType  = reflect.TypeOf(VarString(""))
	varBytesType   = reflect.TypeOf(VarBytes(nil))
	nullStringType = reflect.TypeOf(NullableString(""))
	nullBytesType  = reflect.TypeOf(NullableBytes(nil))
)

func makeUint16(b []byte) uint16 {
	_ = b[1]
	return uint16(b[1])<<8 | uint16(b[0])
}

func makeUint32(b []byte) uint32 {
	_ = b[3]
	return uint32(b[3])<<24 |
		uint32(b[2])<<16 |
		uint32(b[1])<<8 |
		uint32(b[0])
}

func makeUint64(b []byte) uint64 {
	_ = b[7]
	return uint64(b[7])<<56 |
		uint64(b[6])<<48 |
		uint64(b[5])<<40 |
		uint64(b[4])<<32 |
		uint64(b[3])<<24 |
		uint64(b[2])<<16 |
		uint64(b[1])<<8 |
		uint64(b[0])
}

func putUint16(b []byte, i uint16) {
	_ = b[1]
	b[0] = byte(i)
	b[1] = byte(i >> 8)
}

func putUint32(b []byte, i uint32) {
	_ = b[3]
	b[0] = byte(i)
	b[1] = byte(i >> 8)
	b[2] = byte(i >> 16)
	b[3] = byte(i >> 24)
}

func putUint64(b []byte, i uint64) {
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
