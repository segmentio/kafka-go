package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"time"

	"github.com/segmentio/kafka-go/compress"
)

// Attributes is a bitset representing special attributes set on records.
type Attributes int16

const (
	Gzip          Attributes = Attributes(compress.Gzip)   // 1
	Snappy        Attributes = Attributes(compress.Snappy) // 2
	Lz4           Attributes = Attributes(compress.Lz4)    // 3
	Zstd          Attributes = Attributes(compress.Zstd)   // 4
	Transactional Attributes = 1 << 4
	ControlBatch  Attributes = 1 << 5
)

func (a Attributes) Compression() compress.Compression {
	return compress.Compression(a & 7)
}

func (a Attributes) Transactional() bool {
	return (a & Transactional) != 0
}

func (a Attributes) ControlBatch() bool {
	return (a & ControlBatch) != 0
}

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
	//crc crc32Hash
}

// Close closes all records of rs.
func (rs *RecordSet) Close() error {
	for i := range rs.Records {
		r := &rs.Records[i]
		r.Close()
	}
	return nil
}

const magicByteOffset = 16

// ReadFrom reads the representation of a record set from r into rs, returning
// the number of bytes consumed from r, and an non-nil error if the record set
// could not be read.
func (rs *RecordSet) ReadFrom(r io.Reader) (int64, error) {
	var size int32
	var magic byte
	var rn int64

	if rb, _ := r.(*bufio.Reader); rb != nil {
		// This is an optimized code path for the common case where we're
		// reading from a bufio.Reader, we use the Peek method to get a pointer
		// to the internal buffer of the reader and avoid allocating a temporary
		// area to store the data.
		b, err := rb.Peek(4)
		// We need to consume the size, because the readFromVersion* methods
		// expect to be positionned at the beginning of the message set or
		// record batch.
		n, _ := rb.Discard(len(b))
		if err != nil {
			return int64(n), dontExpectEOF(err)
		}

		if size = readInt32(b); size <= 0 {
			*rs = RecordSet{}
			return int64(n), nil
		}

		c, err := rb.Peek(magicByteOffset + 1)
		if err != nil {
			rb.Discard(len(c))
			return int64(n + len(c)), dontExpectEOF(err)
		}
		magic = c[magicByteOffset]
		rn = int64(n)
	} else {
		// Less optimal code path, we need to allocate a temporary buffer to
		// read the size and magic byte from the reader.
		b := make([]byte, 4+magicByteOffset+1)

		if n, err := io.ReadFull(r, b[:4]); err != nil {
			return int64(n), dontExpectEOF(err)
		}

		if size = readInt32(b[:4]); size <= 0 {
			*rs = RecordSet{}
			return 4, nil
		}

		if n, err := io.ReadFull(r, b[4:]); err != nil {
			return 4 + int64(n), dontExpectEOF(err)
		}
		magic = b[magicByteOffset]
		// Reconstruct the prefix that we just had to consume from the reader
		// to pass the data to the right read method based on the magic number
		// that we got. Note that we offset the buffer by 4 bytes to skip the
		// size because the readFromVersion* methods expect to be positioned at
		// the beginning of the message set or record batch.
		r = io.MultiReader(bytes.NewReader(b[4:]), r)
		rn = int64(len(b))
	}

	var n int64
	var err error
	switch magic {
	case 0, 1:
		n, err = rs.readFromVersion1(r, int(size))
	case 2:
		n, err = rs.readFromVersion2(r, int(size))
	default:
		return rn, errorf("unsupported message version %d for message of size %d", magic, size)
	}
	return 4 + n, dontExpectEOF(err)
}

func (rs *RecordSet) readFromVersion1(r io.Reader, size int) (int64, error) {
	crcReader := &crc32Reader{}
	crcReader.reset(r, crc32.IEEETable)

	d := &decoder{
		reader: crcReader,
		remain: size,
	}

	version := int8(0)
	baseOffset := d.readInt64()
	messageSize := d.readInt32()
	fmt.Println("READ: message size =", messageSize, "/", size)

	if d.err != nil {
		return int64(size - d.remain), d.err
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

	baseAttributes := int8(0)
	d.remain = int(messageSize)

	for offset := baseOffset; d.remain > 0; offset++ {
		crc := uint32(d.readInt32())
		crcReader.reset(crcReader.reader, crc32.IEEETable)
		magicByte := d.readInt8()
		attributes := d.readInt8()
		timestamp := int64(0)

		if magicByte != 0 {
			timestamp = d.readInt64()
		}

		if version == 0 {
			version = magicByte
		}

		if offset == baseOffset {
			baseAttributes = attributes
		}

		var compression = Attributes(attributes).Compression()
		var codec compress.Codec
		if compression != 0 {
			if codec = compression.Codec(); codec == nil {
				d.err = errorf("unsupported compression codec: %d", compression)
				break
			}
		}

		var k ByteSequence
		var v ByteSequence

		keyOffset := buffer.Size()
		hasKey := d.readBytesTo(buffer)
		if hasKey {
			k = buffer.ref(keyOffset, buffer.Size())
		}

		valueOffset := buffer.Size()
		valueLength := d.readInt32()
		hasValue := valueLength >= 0

		if hasValue {
			if codec == nil {
				d.writeTo(buffer, int(valueLength))
				v = buffer.ref(valueOffset, buffer.Size())
			} else {
				decompressor := codec.NewReader(d)

				subset := RecordSet{}
				_, d.err = subset.readFromVersion1(decompressor, math.MaxInt32)

				decompressor.Close()

				if d.err != nil {
					break
				}

				unref(k) // is it valid to have a non-nil key on the root message?
				records = append(records, subset.Records...)
			}
		}

		if compression == 0 {
			records = append(records, Record{
				Offset: offset,
				Time:   makeTime(timestamp),
				Key:    k,
				Value:  v,
			})
		}

		if crcReader.sum != crc {
			d.err = errorf("crc32 checksum mismatch (computed=%d found=%d)", crcReader.sum, crc)
			break
		}
	}

	rn := 12 + int64(int(messageSize)-d.remain)

	if d.err != nil {
		return rn, d.err
	}

	*rs = RecordSet{
		Version:    version,
		BaseOffset: baseOffset,
		Records:    records,
		Attributes: Attributes(baseAttributes),
	}

	records = nil
	return rn, nil
}

func (rs *RecordSet) readFromVersion2(r io.Reader, size int) (int64, error) {
	d := decoder{
		reader: r,
		remain: size,
	}

	var (
		baseOffset           = d.readInt64()
		batchLength          = d.readInt32()
		partitionLeaderEpoch = d.readInt32()
		magicByte            = d.readInt8()
		crc                  = d.readInt32()
	)

	crcReader := &crc32Reader{}
	crcReader.reset(d.reader, crc32.MakeTable(crc32.Castagnoli))
	d.reader = crcReader

	var (
		attributes      = d.readInt16()
		lastOffsetDelta = d.readInt32()
		firstTimestamp  = d.readInt64()
		maxTimestamp    = d.readInt64()
		producerID      = d.readInt64()
		producerEpoch   = d.readInt16()
		baseSequence    = d.readInt32()
		numRecords      = d.readInt32()
		reader          = io.Reader(&d)
	)

	_ = batchLength     // batch ends when we reach `size`
	_ = lastOffsetDelta // this is not useful for reading the records
	_ = maxTimestamp    // not sure what this field is useful for

	if compression := Attributes(attributes).Compression(); compression != 0 {
		codec := compression.Codec()
		if codec == nil {
			return int64(size - d.remain), errorf("unsupported compression codec (%d)", compression)
		}
		decompressor := codec.NewReader(reader)
		defer decompressor.Close()
		reader = decompressor
	}

	buffer := newPageBuffer()
	defer buffer.unref()

	_, err := buffer.ReadFrom(reader)
	n := int64(size - d.remain)
	if err != nil {
		return n, err
	}

	fmt.Println("READ: crc =", crcReader.sum)
	if crcReader.sum != uint32(crc) {
		return n, errorf("crc32 checksum mismatch (computed=%d found=%d)", crcReader.sum, uint32(crc))
	}

	recordsLength := buffer.Len()

	d = decoder{
		reader: buffer,
		remain: recordsLength,
	}

	records := make([]Record, numRecords)

	for i := range records {
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
			k = buffer.ref(keyOffset, keyOffset+keyLength)
		}
		if valueLength >= 0 {
			v = buffer.ref(valueOffset, valueOffset+valueLength)
		}

		records[i] = Record{
			Offset:  int64(baseOffset) + offsetDelta,
			Time:    makeTime(int64(firstTimestamp) + timestampDelta),
			Key:     k,
			Value:   v,
			Headers: headers,
		}
	}

	if d.err != nil {
		for _, r := range records {
			unref(r.Key)
			unref(r.Value)
		}
	} else {
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
	}

	d.discardAll()
	return n, d.err
}

// WriteTo writes the representation of rs into w. The value of rs.Version
// dictates which format that the record set will be represented as.
//
// Note: since this package is only compatible with kafka 0.10 and above, the
// method never produces messages in version 0. If rs.Version is zero, the
// method defaults to producing messages in version 1.
func (rs *RecordSet) WriteTo(w io.Writer) (int64, error) {
	// This optimization avoids rendering the record set in an intermediary
	// buffer when the writer is already a pageBuffer, which is a common case
	// due to the way WriteRequest and WriteResponse are implemented.
	buffer, _ := w.(*pageBuffer)
	bufferOffset := int64(0)

	if buffer != nil {
		bufferOffset = buffer.Size()
	} else {
		buffer = newPageBuffer()
		defer buffer.unref()
	}

	size := packUint32(0)
	buffer.Write(size[:]) // size placeholder

	var err error
	switch rs.Version {
	case 0, 1:
		err = rs.writeToVersion1(buffer, bufferOffset+4)
	case 2:
		err = rs.writeToVersion2(buffer, bufferOffset+4)
	default:
		err = errorf("unsupported record set version %d", rs.Version)
	}
	if err != nil {
		return 0, err
	}

	n := buffer.Size() - bufferOffset
	if n == 0 {
		size = packUint32(^uint32(0))
	} else {
		size = packUint32(uint32(n) - 4)
	}
	buffer.WriteAt(size[:], bufferOffset)

	if buffer != w {
		return buffer.WriteTo(w)
	}

	return n, nil
}

func (rs *RecordSet) writeToVersion1(buffer *pageBuffer, bufferOffset int64) error {
	attributes := rs.Attributes
	records := rs.Records

	if len(records) == 0 {
		return nil
	}

	if compression := attributes.Compression(); compression != 0 && len(records) != 0 {
		fmt.Println("COMPRESS")
		if codec := compression.Codec(); codec != nil {
			subset := *rs
			subset.Attributes &= ^7 // erase compression

			if err := subset.writeToVersion1(buffer, bufferOffset); err != nil {
				return err
			}

			compressed := newPageBuffer()
			defer compressed.unref()

			compressor := codec.NewWriter(compressed)
			defer compressor.Close()

			var err error
			buffer.pages.scan(bufferOffset, buffer.Size(), func(b []byte) bool {
				_, err = compressor.Write(b)
				return err != nil
			})
			if err != nil {
				return err
			}

			if err := compressor.Close(); err != nil {
				return err
			}

			buffer.Truncate(int(bufferOffset))

			records = []Record{{
				Offset: records[0].Offset,
				Time:   records[0].Time,
				Value:  compressed,
			}}
		}
	}

	crcWriter := &crc32Writer{}
	crcWriter.reset(buffer, crc32.IEEETable)

	e := encoder{writer: crcWriter}
	e.writeInt64(records[0].Offset)
	e.writeInt32(0) // message set size placeholder

	for i := range records {
		r := &records[i]

		recordOffset := buffer.Size()
		e.writeInt32(0) // crc32 placeholder
		crcWriter.reset(crcWriter.writer, crc32.IEEETable)
		e.writeInt8(1) // magic byte: version 1
		e.writeInt8(int8(attributes))
		e.writeInt64(timestamp(r.Time))

		if err := e.writeNullBytesFrom(r.Key); err != nil {
			return err
		}

		if err := e.writeNullBytesFrom(r.Value); err != nil {
			return err
		}

		fmt.Printf("WRITE: crc32 (v1) = %#08x @%d\n", crcWriter.sum, recordOffset)
		b := packUint32(crcWriter.sum)
		buffer.WriteAt(b[:], recordOffset)
	}

	totalLength := buffer.Size() - bufferOffset
	fmt.Println("WRITE: message size (v1) =", totalLength-12)
	b := packUint32(uint32(totalLength) - 12)
	buffer.WriteAt(b[:], bufferOffset+8)
	return nil
}

func (rs *RecordSet) writeToVersion2(buffer *pageBuffer, bufferOffset int64) error {
	records := rs.Records
	baseOffset := rs.BaseOffset
	numRecords := int32(len(records))

	if numRecords == 0 {
		return nil
	}

	r0, r1 := &records[0], &records[numRecords-1]
	lastOffsetDelta := int32(r1.Offset - r0.Offset)
	firstTimestamp := timestamp(r0.Time)
	maxTimestamp := timestamp(r1.Time)

	e := encoder{writer: buffer}
	e.writeInt64(baseOffset)
	e.writeInt32(0) // placeholder for record batch length
	e.writeInt32(rs.PartitionLeaderEpoch)
	e.writeInt8(2)  // magic byte
	e.writeInt32(0) // placeholder for crc32 checksum

	crcWriter := &crc32Writer{}
	crcWriter.reset(e.writer, crc32.MakeTable(crc32.Castagnoli))
	e.writer = crcWriter

	e.writeInt16(int16(rs.Attributes))
	e.writeInt32(lastOffsetDelta)
	e.writeInt64(firstTimestamp)
	e.writeInt64(maxTimestamp)
	e.writeInt64(rs.ProducerID)
	e.writeInt16(rs.ProducerEpoch)
	e.writeInt32(rs.BaseSequence)
	e.writeInt32(numRecords)

	var compressor io.WriteCloser
	if compression := rs.Attributes.Compression(); compression != 0 {
		if codec := compression.Codec(); codec != nil {
			compressor = codec.NewWriter(e.writer)
			defer compressor.Close()
			e.writer = compressor
		}
	}

	for i := range records {
		r := &records[i]
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
			return err
		}

		if err := e.writeCompactNullBytesFrom(r.Value); err != nil {
			return err
		}

		e.writeVarInt(int64(len(r.Headers)))

		for _, h := range r.Headers {
			e.writeCompactString(h.Key)
			e.writeCompactNullBytes(h.Value)
		}
	}

	if compressor != nil {
		if err := compressor.Close(); err != nil {
			return err
		}
	}

	totalLength := buffer.Size() - bufferOffset
	batchLength := totalLength - 12

	length := packUint32(uint32(batchLength))
	buffer.WriteAt(length[:], bufferOffset+8)

	fmt.Println("WRITE: crc =", crcWriter.sum)
	checksum := packUint32(crcWriter.sum)
	buffer.WriteAt(checksum[:], bufferOffset+17)
	return nil
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
	binary.BigEndian.PutUint32(b[:], u)
	return
}

func readInt8(b []byte) int8 {
	return int8(b[0])
}

func readInt16(b []byte) int16 {
	return int16(binary.BigEndian.Uint16(b))
}

func readInt32(b []byte) int32 {
	return int32(binary.BigEndian.Uint32(b))
}

func readInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

func writeInt8(b []byte, i int8) {
	b[0] = byte(i)
}

func writeInt16(b []byte, i int16) {
	binary.BigEndian.PutUint16(b, uint16(i))
}

func writeInt32(b []byte, i int32) {
	binary.BigEndian.PutUint32(b, uint32(i))
}

func writeInt64(b []byte, i int64) {
	binary.BigEndian.PutUint64(b, uint64(i))
}

func dontExpectEOF(err error) error {
	switch err {
	case nil:
		return nil
	case io.EOF:
		return io.ErrUnexpectedEOF
	default:
		return err
	}
}
