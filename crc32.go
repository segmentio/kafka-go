package kafka

import (
	"encoding/binary"
	"hash/crc32"
)

func crc32Int8(sum uint32, v int8) uint32 {
	b := [1]byte{byte(v)}
	return crc32Add(sum, b[:])
}

func crc32Int32(sum uint32, v int32) uint32 {
	b := [4]byte{}
	binary.BigEndian.PutUint32(b[:], uint32(v))
	return crc32Add(sum, b[:])
}

func crc32Int64(sum uint32, v int64) uint32 {
	b := [8]byte{}
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return crc32Add(sum, b[:])
}

func crc32Bytes(sum uint32, b []byte) uint32 {
	if b == nil {
		sum = crc32Int32(sum, -1)
	} else {
		sum = crc32Int32(sum, int32(len(b)))
	}
	return crc32Add(sum, b)
}

func crc32Add(sum uint32, b []byte) uint32 {
	return crc32.Update(sum, crc32.IEEETable, b[:])
}
