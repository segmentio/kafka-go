package protocol

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

var (
	crc32Castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

type crc32Reader struct {
	reader io.Reader
	table  *crc32.Table
	sum    uint32
}

func (crc *crc32Reader) reset(r io.Reader, t *crc32.Table) {
	crc.reader, crc.table, crc.sum = r, t, 0
}

func (crc *crc32Reader) update(b []byte) {
	crc.sum = crc32.Update(crc.sum, crc.table, b)
}

func (crc *crc32Reader) Read(b []byte) (int, error) {
	n, err := crc.reader.Read(b)
	crc.update(b[:n])
	return n, err
}

type crc32Writer struct {
	writer io.Writer
	table  *crc32.Table
	sum    uint32
}

func (crc *crc32Writer) reset(w io.Writer, t *crc32.Table) {
	crc.writer, crc.table, crc.sum = w, t, 0
}

func (crc *crc32Writer) update(b []byte) {
	sum := crc.sum
	crc.sum = crc32.Update(crc.sum, crc.table, b)
	fmt.Printf("CRC32: %#08x + %q => %#08x\n", sum, b, crc.sum)
}

func (crc *crc32Writer) Write(b []byte) (int, error) {
	n, err := crc.writer.Write(b)
	crc.update(b[:n])
	return n, err
}

type crc32Hash struct {
	buffer [8]byte
	table  *crc32.Table
	sum    uint32
}

func (h *crc32Hash) reset(table *crc32.Table) {
	h.sum, h.table = 0, table
}

func (h *crc32Hash) write(b []byte) {
	h.sum = crc32.Update(h.sum, h.table, b)
}

func (h *crc32Hash) writeInt8(i int8) {
	h.buffer[0] = byte(i)
	h.write(h.buffer[:1])
}

func (h *crc32Hash) writeInt16(i int16) {
	binary.BigEndian.PutUint16(h.buffer[:2], uint16(i))
	h.write(h.buffer[:2])
}

func (h *crc32Hash) writeInt32(i int32) {
	binary.BigEndian.PutUint32(h.buffer[:4], uint32(i))
	h.write(h.buffer[:4])
}

func (h *crc32Hash) writeInt64(i int64) {
	binary.BigEndian.PutUint64(h.buffer[:8], uint64(i))
	h.write(h.buffer[:8])
}

func (h *crc32Hash) writeNullBytes(b ByteSequence) {
	if b == nil {
		h.writeInt32(-1)
	} else {
		h.writeInt32(int32(b.Size()))
		copyBytes(h, b)
	}
}

func (h *crc32Hash) Write(b []byte) (int, error) {
	h.write(b)
	return len(b), nil
}
