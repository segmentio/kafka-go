package protocol

import (
	"encoding/binary"
	"hash/crc32"
)

var (
	crc32Table = crc32.MakeTable(crc32.Castagnoli)
)

type crc32Hash struct {
	//writer io.Writer
	buffer [8]byte
	sum    uint32
}

func (h *crc32Hash) reset() {
	h.sum = 0
}

func (h *crc32Hash) write(b []byte) {
	h.sum = crc32.Update(h.sum, crc32Table, b)
}

func (h *crc32Hash) writeInt8(i int8) {
	h.buffer[0] = byte(i)
	h.write(h.buffer[:1])
}

func (h *crc32Hash) writeInt16(i int16) {
	binary.LittleEndian.PutUint16(h.buffer[:2], uint16(i))
	h.write(h.buffer[:2])
}

func (h *crc32Hash) writeInt32(i int32) {
	binary.LittleEndian.PutUint32(h.buffer[:4], uint32(i))
	h.write(h.buffer[:4])
}

func (h *crc32Hash) writeInt64(i int64) {
	binary.LittleEndian.PutUint64(h.buffer[:8], uint64(i))
	h.write(h.buffer[:8])
}

func (h *crc32Hash) writeNullBytes(p ByteSequence) {
	if p == nil {
		h.writeInt32(-1)
	} else {
		h.writeInt32(int32(p.Size()))
		copyBytes(h, p)
	}
}

func (h *crc32Hash) Write(b []byte) (int, error) {
	h.write(b)
	return len(b), nil
}
