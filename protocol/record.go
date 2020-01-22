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

// Attributes is a bitset representing special attributes set on records.
type Attributes int16

// Header represents a single entry in a list of record headers.
type Header struct {
	Key   string
	Value []byte
}

// Record values represent single records exchanged in Produce requests and
// Fetch responses.
type Record struct {
	Offset  int64
	Time    time.Time
	Key     ByteSequence
	Value   ByteSequence
	Headers []Header
}

// Close closes both the key and value of the record.
func (r *Record) Close() error {
	if r.Key != nil {
		r.Key.Close()
	}
	if r.Value != nil {
		r.Value.Close()
	}
	return nil
}

// RecordSet represents a sequence of records in Produce requests and Fetch
// responses. All v0, v1, and v2 formats are supported.
type RecordSet struct {
	// The message version that this record set will be represented as, valid
	// values are 1, or 2.
	Version int8
	// The following fields carry properties used when representing the record
	// batch in version 2.
	Attributes           Attributes
	PartitionLeaderEpoch int32
	BaseOffset           int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	// The list of records contained in this set.
	Records []Record
	// This unexported field is used internally to compute crc32 hashes of the
	// record set during serialization and deserialization.
	crc crc32Hash
}

// Close closes all records of rs.
func (rs *RecordSet) Close() error {
	for i := range rs.Records {
		r := &rs.Records[i]
		r.Close()
	}
	return nil
}

const headerSize = 8 + 4 // sizeof(baseOffset) + sizeof(messageSetSize)
const magicByteOffset = 16
const messageSizeOffset = 8

// ReadFrom reads the representation of a record set from r into rs, returning
// the number of bytes consumed from r, and an non-nil error if the record set
// could not be read.
func (rs *RecordSet) ReadFrom(r io.Reader) (int64, error) {
	var baseOffset int64
	var messageSize int32
	var rn int64

	rb, _ := r.(*bufio.Reader)
	if rb != nil {
		b, err := rb.Peek(headerSize)
		if err != nil {
			return 0, err
		}
		baseOffset = readInt64(b[:8])
		messageSize = readInt32(b[8:12])
	} else {
		b := make([]byte, headerSize)

		if n, err := io.ReadFull(r, b); err != nil {
			return int64(n), err
		}

		baseOffset = readInt64(b[:8])
		messageSize = readInt32(b[8:12])
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

	version := int8(0)
	baseOffset := d.readInt64()
	messageSize := d.readInt32()

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

	buffer := newPageBuffer()
	defer buffer.unref()

	d.remain = int(messageSize)

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
			break
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

		_ = attributes // TODO
	}

	*rs = RecordSet{
		Version:    version,
		BaseOffset: baseOffset,
		Records:    records,
	}

	records = nil
	return headerSize + int64(int(messageSize)-d.remain), d.err
}

func (rs *RecordSet) readFromVersion2(r *bufio.Reader) (int64, error) {
	b, err := r.Peek(57)
	if err != nil {
		return 0, err
	}

	_ = b[56] // bound check elimination
	var (
		baseOffset           = readInt64(b[0:])
		batchLength          = readInt32(b[8:])
		partitionLeaderEpoch = readInt32(b[12:])
		magicByte            = readInt8(b[16:])
		crc                  = readInt32(b[17:])
		attributes           = readInt16(b[21:])
		lastOffsetDelta      = readInt32(b[23:])
		firstTimestamp       = readInt64(b[27:])
		maxTimestamp         = readInt64(b[35:])
		producerID           = readInt64(b[43:])
		producerEpoch        = readInt16(b[51:])
		baseSequence         = readInt32(b[53:])
	)
	_ = maxTimestamp // not sure what this field is useful for

	rs.crc.reset()
	rs.crc.write(b[21:])

	r.Discard(57)

	buffer := newPageBuffer()
	defer buffer.unref()

	reader := acquireBufferedReader(r, int64(batchLength)-45)
	defer releaseBufferedReader(reader)

	n, err := buffer.ReadFrom(reader.Reader)
	if err != nil {
		return 57 + n, err
	}
	if n != (int64(batchLength) - 45) {
		return 57 + n, io.ErrUnexpectedEOF
	}

	buffer.pages.scan(0, int64(n), func(b []byte) bool {
		rs.crc.write(b)
		return true
	})

	if rs.crc.sum != uint32(crc) {
		return 57 + n, errorf("crc32 checksum mismatch (computed=%d found=%d)", rs.crc.sum, uint32(crc))
	}

	reader.Reset(buffer)
	recordsLength := buffer.Len()

	d := decoder{
		reader: reader.Reader,
		remain: recordsLength,
	}

	numRecords := lastOffsetDelta
	if numRecords > 1e6 {
		// Set a hard limit at 1M records in case the wire representation is
		// malformed and the offset delta was invalid.
		numRecords = 1e6
	}

	records := make([]Record, 0, numRecords)
	defer func() {
		for _, r := range records {
			unref(r.Key)
			unref(r.Value)
		}
	}()

	for d.remain > 0 {
		_ = d.readVarInt() // useless length of the record
		_ = d.readInt8()   // record attributes (unused)
		timestampDelta := d.readVarInt()
		offsetDelta := d.readVarInt()

		keyLength := d.readVarInt()
		keyOffset := int64(recordsLength - d.remain)
		if keyLength > 0 {
			d.discard(int(keyLength))
		}

		valueLength := d.readVarInt()
		valueOffset := int64(recordsLength - d.remain)
		if valueLength > 0 {
			d.discard(int(valueLength))
		}

		numHeaders := d.readVarInt()
		headers := ([]Header)(nil)

		if numHeaders > 0 {
			headers = make([]Header, numHeaders)

			for i := range headers {
				headers[i] = Header{
					Key:   d.readCompactString(),
					Value: d.readCompactBytes(),
				}
			}
		}

		if d.err != nil {
			break
		}

		var k ByteSequence
		var v ByteSequence
		if keyLength >= 0 {
			k = buffer.ref(keyOffset, valueOffset)
		}
		if valueLength >= 0 {
			v = buffer.ref(valueOffset, valueLength)
		}

		records = append(records, Record{
			Offset:  int64(baseOffset) + offsetDelta,
			Time:    makeTime(int64(firstTimestamp) + timestampDelta),
			Key:     k,
			Value:   v,
			Headers: headers,
		})
	}

	*rs = RecordSet{
		Version:              int8(magicByte),
		Attributes:           Attributes(attributes),
		PartitionLeaderEpoch: int32(partitionLeaderEpoch),
		BaseOffset:           int64(baseOffset),
		ProducerID:           int64(producerID),
		ProducerEpoch:        int16(producerEpoch),
		BaseSequence:         int32(baseSequence),
		Records:              records,
	}

	records = nil
	return 57 + n, d.err
}

// WriteTo writes the representation of rs into w. The value of rs.Version
// dictates which format that the record set will be represented as.
//
// Note: since this package is only compatible with kafka 0.10 and above, the
// method never produces messages in version 0. If rs.Version is zero, the
// method defaults to producing messages in version 1.
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
		attributes := int8(0)
		timestamp := timestamp(r.Time)
		e.writeInt32(0) // crc32 placeholder
		e.writeInt8(1)  // magic byte: version 1
		e.writeInt8(attributes)
		e.writeInt64(timestamp)

		if err := e.writeNullBytesFrom(r.Key); err != nil {
			return 0, err
		}

		if err := e.writeNullBytesFrom(r.Value); err != nil {
			return 0, err
		}

		rs.crc.reset()
		rs.crc.writeInt8(1)
		rs.crc.writeInt8(attributes)
		rs.crc.writeInt64(timestamp)
		rs.crc.writeNullBytes(r.Key)
		rs.crc.writeNullBytes(r.Value)

		b := packUint32(rs.crc.sum)
		buffer.WriteAt(b[:], int64(i))
	}

	if n := buffer.Len(); n == 0 {
		e.writeInt64(0) // offset
		e.writeInt32(0) // message set size
	} else {
		b := packUint32(uint32(n) - headerSize)
		buffer.WriteAt(b[:], messageSizeOffset)
	}

	return buffer.WriteTo(w)
}

func (rs *RecordSet) writeToVersion2(w io.Writer) (int64, error) {
	buffer := newPageBuffer()
	defer buffer.unref()

	baseOffset := rs.BaseOffset
	lastOffsetDelta := int32(0)
	firstTimestamp := int64(0)
	maxTimestamp := int64(0)

	if records := rs.Records; len(records) != 0 {
		last := len(records) - 1
		lastOffsetDelta = int32(records[last].Offset - baseOffset)
		firstTimestamp = timestamp(records[0].Time)
		maxTimestamp = timestamp(records[last].Time)
	}

	b := [57]byte{}
	writeInt64(b[0:], baseOffset)
	writeInt32(b[8:], 0) // placeholder for record batch length
	writeInt32(b[12:], rs.PartitionLeaderEpoch)
	writeInt8(b[16:], 2)  // magic byte
	writeInt32(b[17:], 0) // placeholder for crc32 checksum
	writeInt16(b[21:], int16(rs.Attributes))
	writeInt32(b[23:], lastOffsetDelta)
	writeInt64(b[27:], firstTimestamp)
	writeInt64(b[35:], maxTimestamp)
	writeInt64(b[43:], rs.ProducerID)
	writeInt16(b[51:], rs.ProducerEpoch)
	writeInt32(b[53:], rs.BaseSequence)
	buffer.Write(b[:])

	e := encoder{writer: buffer}

	for _, r := range rs.Records {
		timestampDelta := timestamp(r.Time) - firstTimestamp
		offsetDelta := r.Offset - baseOffset
		keyLength := byteSequenceLength(r.Key)
		valueLength := byteSequenceLength(r.Value)

		length := 1 + // attributes
			sizeOfVarInt(timestampDelta) +
			sizeOfVarInt(offsetDelta) +
			sizeOfVarInt(keyLength) +
			sizeOfVarInt(valueLength) +
			sizeOfVarInt(int64(len(r.Headers))) +
			int(keyLength) +
			int(valueLength)

		for _, h := range r.Headers {
			length += sizeOfCompactString(h.Key) + sizeOfCompactNullBytes(h.Value)
		}

		e.writeVarInt(int64(length))
		e.writeInt8(0) // record attributes (unused)
		e.writeVarInt(timestampDelta)
		e.writeVarInt(offsetDelta)

		if err := e.writeCompactNullBytesFrom(r.Key); err != nil {
			return 0, err
		}

		if err := e.writeCompactNullBytesFrom(r.Value); err != nil {
			return 0, err
		}

		e.writeVarInt(int64(len(r.Headers)))

		for _, h := range r.Headers {
			e.writeCompactString(h.Key)
			e.writeCompactNullBytes(h.Value)
		}
	}

	bufferLength := buffer.Len()
	batchLength := bufferLength - 12

	rs.crc.reset()
	buffer.pages.scan(21, int64(bufferLength), func(b []byte) bool {
		rs.crc.write(b)
		return true
	})

	length := packUint32(uint32(batchLength))
	buffer.WriteAt(length[:], 8)

	checksum := packUint32(rs.crc.sum)
	buffer.WriteAt(checksum[:], 17)

	return buffer.WriteTo(w)
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

func byteSequenceLength(b ByteSequence) int64 {
	if b == nil {
		return -1
	}
	return b.Size()
}

func packUint32(u uint32) (b [4]byte) {
	binary.LittleEndian.PutUint32(b[:], u)
	return
}

func readInt8(b []byte) int8 {
	return int8(b[0])
}

func readInt16(b []byte) int16 {
	return int16(binary.LittleEndian.Uint16(b))
}

func readInt32(b []byte) int32 {
	return int32(binary.LittleEndian.Uint32(b))
}

func readInt64(b []byte) int64 {
	return int64(binary.LittleEndian.Uint64(b))
}

func writeInt8(b []byte, i int8) {
	b[0] = byte(i)
}

func writeInt16(b []byte, i int16) {
	binary.LittleEndian.PutUint16(b, uint16(i))
}

func writeInt32(b []byte, i int32) {
	binary.LittleEndian.PutUint32(b, uint32(i))
}

func writeInt64(b []byte, i int64) {
	binary.LittleEndian.PutUint64(b, uint64(i))
}
