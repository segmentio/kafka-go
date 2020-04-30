package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
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

const magicByteOffset = 16

// ReadFrom reads the representation of a record set from r into rs, returning
// the number of bytes consumed from r, and an non-nil error if the record set
// could not be read.
func (rs *RecordSet) ReadFrom(r io.Reader) (int64, error) {
	var size int32
	var magic byte
	var rn int64

	rb, _ := r.(*bufio.Reader)
	if rb != nil {
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

		fmt.Println("read size:", size)

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
		prefix := bytes.NewReader(b[4:])
		reader := io.MultiReader(prefix, r)
		rb = bufio.NewReader(reader)
		rn = int64(len(b))
	}

	var n int64
	var err error
	switch magic {
	case 0, 1:
		n, err = rs.readFromVersion1(rb, size)
	case 2:
		n, err = rs.readFromVersion2(rb, size)
	default:
		return rn, fmt.Errorf("unsupported message version %d for message of size %d", magic, size)
	}
	return 4 + n, dontExpectEOF(err)
}

func (rs *RecordSet) readFromVersion1(r *bufio.Reader, size int32) (int64, error) {
	d := decoder{
		reader: r,
		remain: int(size),
	}

	version := int8(0)
	baseOffset := d.readInt64()
	messageSize := d.readInt32()

	if d.err != nil {
		return int64(int(size) - d.remain), d.err
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
	baseAttributes := int8(0)

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

		if offset == baseOffset {
			baseAttributes = attributes
		}

		keyOffset := buffer.Size()
		hasKey := d.readBytesTo(buffer)

		valueOffset := buffer.Size()
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
			v = buffer.ref(valueOffset, buffer.Size())
		}

		rs.crc.reset()
		rs.crc.writeInt8(magicByte)
		rs.crc.writeInt8(attributes)
		rs.crc.writeInt64(timestamp)
		rs.crc.writeNullBytes(k)
		rs.crc.writeNullBytes(v)

		if rs.crc.sum != crc {
			fmt.Println("magicByte =", magicByte)
			fmt.Println("attributes =", attributes)
			fmt.Println("timestamp =", timestamp)
			fmt.Println(k)
			fmt.Println(v)
			d.err = errorf("crc32 checksum mismatch (computed=%d found=%d)", rs.crc.sum, crc)
			break
		}

		if compression := Attributes(attributes).Compression(); compression != 0 && hasValue {
			if codec := compression.Codec(); codec != nil {
				decompressor := codec.NewReader(v)
				decompressed := acquireBufferedReader(decompressor, -1)

				subset := RecordSet{}
				_, d.err = subset.readFromVersion1(decompressed.Reader, math.MaxInt32)

				releaseBufferedReader(decompressed)
				decompressor.Close()

				if d.err != nil {
					break
				}

				records = append(records, subset.Records...)
				continue
			}
		}

		records = append(records, Record{
			Offset: offset,
			Time:   makeTime(timestamp),
			Key:    k,
			Value:  v,
		})
	}

	*rs = RecordSet{
		Version:    version,
		BaseOffset: baseOffset,
		Records:    records,
		Attributes: Attributes(baseAttributes),
	}

	records = nil
	return 12 + int64(int(messageSize)-d.remain), d.err
}

func (rs *RecordSet) readFromVersion2(r *bufio.Reader, size int32) (int64, error) {
	b, err := r.Peek(61)
	if err != nil {
		n, _ := r.Discard(len(b))
		return int64(n), err
	}

	_ = b[60] // bound check elimination
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
		numRecords           = readInt32(b[57:])
	)
	fmt.Println("reading", numRecords, "records")
	_ = lastOffsetDelta // this is not useful for reading the records
	_ = maxTimestamp    // not sure what this field is useful for

	rs.crc.reset()
	rs.crc.write(b[21:])

	r.Discard(61)

	buffer := newPageBuffer()
	defer buffer.unref()

	reader := acquireBufferedReader(r, int64(batchLength)-49)
	defer releaseBufferedReader(reader)

	n, err := buffer.ReadFrom(reader.Reader)
	if err != nil {
		return 61 + n, err
	}
	if n != (int64(batchLength) - 49) {
		return 61 + n, io.ErrUnexpectedEOF
	}

	buffer.pages.scan(0, int64(n), func(b []byte) bool {
		rs.crc.write(b)
		return true
	})

	if rs.crc.sum != uint32(crc) {
		return 61 + n, errorf("crc32 checksum mismatch (computed=%d found=%d)", rs.crc.sum, uint32(crc))
	}

	if compression := Attributes(attributes).Compression(); compression != 0 {
		codec := compression.Codec()
		if codec == nil {
			return 61 + n, errorf("unsupported compression codec (%d)", compression)
		}

		decompressor := codec.NewReader(buffer)
		defer decompressor.Close()

		uncompressed := newPageBuffer()
		defer uncompressed.unref()

		if _, err := uncompressed.ReadFrom(decompressor); err != nil {
			return 61 + n, err
		}

		buffer = uncompressed
	}

	reader.Reset(buffer)
	recordsLength := buffer.Len()

	d := decoder{
		reader: reader.Reader,
		remain: recordsLength,
	}

	records := make([]Record, numRecords)

	for i := range records {
		fmt.Println("read record", i)
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

	fmt.Println("done reading records", d.remain, d.err)

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

	return 61 + n, d.err
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
		err = fmt.Errorf("unsupported record set version %d", rs.Version)
	}
	if err != nil {
		return 0, err
	}

	n := buffer.Size() - bufferOffset
	fmt.Println("buffer size =", n)
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

	e := encoder{writer: buffer}

	for i := range records {
		r := &records[i]

		if i == 0 {
			e.writeInt64(r.Offset)
			e.writeInt32(0) // message set size placeholder
		}

		recordOffset := buffer.Size()
		attributes := int8(attributes)
		timestamp := timestamp(r.Time)
		e.writeInt32(0) // crc32 placeholder
		e.writeInt8(1)  // magic byte: version 1
		e.writeInt8(attributes)
		e.writeInt64(timestamp)

		if err := e.writeNullBytesFrom(r.Key); err != nil {
			return err
		}

		if err := e.writeNullBytesFrom(r.Value); err != nil {
			return err
		}

		fmt.Println("magic Bytes =", 1)
		fmt.Println("attributes =", attributes)
		fmt.Println("timestamp =", timestamp)
		fmt.Println(r.Key)
		fmt.Println(r.Value)

		rs.crc.reset()
		rs.crc.writeInt8(1)
		rs.crc.writeInt8(attributes)
		rs.crc.writeInt64(timestamp)
		rs.crc.writeNullBytes(r.Key)
		rs.crc.writeNullBytes(r.Value)

		fmt.Println("write crc32:", rs.crc.sum)
		b := packUint32(rs.crc.sum)
		buffer.WriteAt(b[:], recordOffset)
	}

	totalLength := buffer.Size() - bufferOffset
	b := packUint32(uint32(totalLength) - 12)
	buffer.WriteAt(b[:], bufferOffset+8)
	return nil
}

func (rs *RecordSet) writeToVersion2(buffer *pageBuffer, bufferOffset int64) error {
	records := rs.Records
	baseOffset := rs.BaseOffset
	lastOffsetDelta := int32(0)
	firstTimestamp := int64(0)
	maxTimestamp := int64(0)
	numRecords := int32(len(records))

	if numRecords == 0 {
		return nil
	}

	if numRecords > 0 {
		r0, r1 := &records[0], &records[numRecords-1]
		lastOffsetDelta = int32(r1.Offset - r0.Offset)
		firstTimestamp = timestamp(r0.Time)
		maxTimestamp = timestamp(r1.Time)
	}

	fmt.Println("writing", numRecords, "records")
	fmt.Println("baseOffset =", baseOffset)
	fmt.Println("partitionLeaderEpoch =", rs.PartitionLeaderEpoch)
	fmt.Println("attributes =", rs.Attributes)
	fmt.Println("lastOffsetDelta =", lastOffsetDelta)
	fmt.Println("firstTimestamp =", firstTimestamp)
	fmt.Println("maxTimestamp =", maxTimestamp)
	fmt.Println("producerID =", rs.ProducerID)
	fmt.Println("producerEpoch =", rs.ProducerEpoch)
	fmt.Println("baseSequence =", rs.BaseSequence)
	fmt.Println("numRecords =", numRecords)

	var b [61]byte
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
	writeInt32(b[57:], numRecords)
	buffer.Write(b[:])

	var e = encoder{writer: buffer}
	var compressor io.WriteCloser

	if compression := rs.Attributes.Compression(); compression != 0 {
		if codec := compression.Codec(); codec != nil {
			compressor = codec.NewWriter(buffer)
			defer compressor.Close()
			e.writer = compressor
		}
	}

	for i := range records {
		r := &records[i]
		fmt.Println("writing record:", r)
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

	rs.crc.reset()
	buffer.pages.scan(bufferOffset+21, bufferOffset+totalLength, func(b []byte) bool {
		rs.crc.write(b)
		return true
	})

	fmt.Println("total length =", totalLength)
	fmt.Println("batch length =", batchLength)

	length := packUint32(uint32(batchLength))
	buffer.WriteAt(length[:], bufferOffset+8)

	checksum := packUint32(rs.crc.sum)
	buffer.WriteAt(checksum[:], bufferOffset+17)

	fmt.Println("optimized buffer write of", totalLength)
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
