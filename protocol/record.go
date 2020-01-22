package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"
)

type Record struct {
	Offset int64
	Time   time.Time
	Key    ByteSequence
	Value  ByteSequence
}

type RecordSet struct {
	Version    int8
	BaseOffset int64
	Records    []Record

	crc crc32Hash
}

func (rs *RecordSet) Close() error {
	for i := range rs.Records {
		r := &rs.Records[i]
		if r.Key != nil {
			r.Key.Close()
		}
		if r.Value != nil {
			r.Value.Close()
		}
	}
	return nil
}

const headerSize = 8 + 4 // sizeof(baseOffset) + sizeof(messageSetSize)
const magicByteOffset = 16
const messageSizeOffset = 8

func (rs *RecordSet) ReadFrom(r io.Reader) (int64, error) {
	var baseOffset uint64
	var messageSize uint32
	var rn int64

	rb, _ := r.(*bufio.Reader)
	if rb != nil {
		b, err := rb.Peek(headerSize)
		if err != nil {
			return 0, err
		}
		baseOffset = le64u(b[:8])
		messageSize = le32u(b[8:12])
	} else {
		b := make([]byte, headerSize)

		if n, err := io.ReadFull(r, b); err != nil {
			return int64(n), err
		}

		baseOffset = le64u(b[:8])
		messageSize = le32u(b[8:12])
		size := int64(messageSize)
		size += headerSize

		rb = bufio.NewReader(io.LimitReader(io.MultiReader(bytes.NewReader(b), r), size))
		rn = headerSize
	}

	if messageSize == 0 {
		*rs = RecordSet{
			Version:    1,
			BaseOffset: int64(baseOffset),
			Records:    []Record{},
		}
		rb.Discard(headerSize)
		return headerSize, nil
	}

	b, err := rb.Peek(magicByteOffset + 1)
	if err != nil {
		return rn, err
	}

	switch version := b[magicByteOffset]; version {
	case 0, 1:
		return rs.readFromVersion1(rb)
	case 2:
		return rs.readFromVersion2(rb)
	default:
		return int64(len(b)), fmt.Errorf("unsupported message version %d for message of size %d starting at offset %d", version, messageSize, baseOffset)
	}
}

func (rs *RecordSet) readFromVersion1(r *bufio.Reader) (int64, error) {
	d := decoder{
		reader: r,
		remain: math.MaxInt32,
	}

	buffer := newPageBuffer()
	defer buffer.unref()

	version := int8(0)
	baseOffset := d.readInt64()
	messageSize := int(d.readInt32())

	if d.err != nil {
		return int64(math.MaxInt32 - d.remain), d.err
	}

	records := make([]Record, 0, 100)
	defer func() {
		for _, r := range records {
			unref(r.Key)
			unref(r.Value)
		}
	}()

	d.remain = messageSize
	defer d.discardAll()

	for offset := baseOffset; d.remain > 0; offset++ {
		crc := uint32(d.readInt32())
		magicByte := d.readInt8()
		attributes := d.readInt8()
		timestamp := int64(0)

		if magicByte != 0 {
			timestamp = d.readInt64()
		}

		if version == 0 {
			version = magicByte
		}

		keyOffset := int64(buffer.Len())
		hasKey := d.readBytesTo(buffer)

		valueOffset := int64(buffer.Len())
		hasValue := d.readBytesTo(buffer)

		if d.err != nil {
			if d.err == ErrTruncated {
				break
			}
			return headerSize + int64(messageSize-d.remain), d.err
		}

		var k ByteSequence
		var v ByteSequence
		if hasKey {
			k = buffer.ref(keyOffset, valueOffset)
		}
		if hasValue {
			v = buffer.ref(valueOffset, int64(buffer.Len()))
		}

		rs.crc.reset()
		rs.crc.writeInt8(magicByte)
		rs.crc.writeInt8(attributes)
		rs.crc.writeInt64(timestamp)
		rs.crc.writeNullBytes(k)
		rs.crc.writeNullBytes(v)

		if rs.crc.sum != crc {
			d.err = errorf("crc32 checksum mismatch (computed=%d found=%d)", rs.crc.sum, crc)
			break
		}

		records = append(records, Record{
			Offset: offset,
			Time:   makeTime(timestamp),
			Key:    k,
			Value:  v,
		})
	}

	if d.discardAll(); d.err != nil && d.err != ErrTruncated {
		return headerSize + int64(messageSize-d.remain), d.err
	}

	*rs = RecordSet{
		Version:    version,
		BaseOffset: baseOffset,
		Records:    records,
	}

	records = nil
	return headerSize + int64(messageSize), nil
}

func (rs *RecordSet) readFromVersion2(r *bufio.Reader) (int64, error) {
	return 0, nil
}

func (rs *RecordSet) WriteTo(w io.Writer) (int64, error) {
	switch rs.Version {
	case 0, 1:
		return rs.writeToVersion1(w)
	case 2:
		return rs.writeToVersion2(w)
	default:
		return 0, fmt.Errorf("unsupported record set version %d", rs.Version)
	}
}

func (rs *RecordSet) writeToVersion1(w io.Writer) (int64, error) {
	buffer := newPageBuffer()
	defer buffer.unref()

	e := encoder{writer: buffer}

	for _, r := range rs.Records {
		if buffer.Len() == 0 {
			e.writeInt64(r.Offset)
			e.writeInt32(0) // message set size placeholder
		}

		i := buffer.Len()
		t := timestamp(r.Time)
		e.writeInt32(0) // crc32 placeholder
		e.writeInt8(1)  // magic byte: version 1
		e.writeInt8(0)  // attributes
		e.writeInt64(t)

		if err := e.writeNullBytesFrom(r.Key); err != nil {
			return 0, err
		}

		if err := e.writeNullBytesFrom(r.Value); err != nil {
			return 0, err
		}

		rs.crc.reset()
		rs.crc.writeInt8(1)
		rs.crc.writeInt8(0)
		rs.crc.writeInt64(t)
		rs.crc.writeNullBytes(r.Key)
		rs.crc.writeNullBytes(r.Value)

		b := u32le(rs.crc.sum)
		buffer.WriteAt(b[:], int64(i))
	}

	if n := buffer.Len(); n == 0 {
		e.writeInt64(0) // offset
		e.writeInt32(0) // message set size
	} else {
		b := u32le(uint32(n) - headerSize)
		buffer.WriteAt(b[:], messageSizeOffset)
	}

	return buffer.WriteTo(w)
}

func (rs *RecordSet) writeToVersion2(w io.Writer) (int64, error) {
	return 0, nil
}

func timestamp(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano() / int64(time.Millisecond)
}

func makeTime(t int64) time.Time {
	return time.Unix(t/1000, (t%1000)*int64(time.Millisecond))
}

func byteSequence(p *pageRef) ByteSequence {
	if p == nil {
		return nil
	}
	return p
}

func le32u(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

func le64u(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

func u32le(u uint32) (b [4]byte) {
	binary.LittleEndian.PutUint32(b[:], u)
	return
}

func u64le(u uint64) (b [8]byte) {
	binary.LittleEndian.PutUint64(b[:], u)
	return
}
