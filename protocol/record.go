package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
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
}

// Close closes all records of rs.
func (rs *RecordSet) Close() error {
	for i := range rs.Records {
		r := &rs.Records[i]
		r.Close()
	}
	return nil
}

// bufferedReader is an interface implemented by types like bufio.Reader, which
// we use to optimize prefix reads by accessing the internal buffer directly
// through calls to Peek.
type bufferedReader interface {
	Discard(int) (int, error)
	Peek(int) ([]byte, error)
}

// bytesBuffer is an interface implemented by types like bytes.Buffer, which we
// use to optimize prefix reads by accessing the internal buffer directly
// through calls to Bytes.
type bytesBuffer interface {
	Bytes() []byte
}

// magicByteOffset is the postion of the magic byte in all versions of record
// sets in the kafka protocol.
const magicByteOffset = 16

// ReadFrom reads the representation of a record set from r into rs, returning
// the number of bytes consumed from r, and an non-nil error if the record set
// could not be read.
func (rs *RecordSet) ReadFrom(r io.Reader) (int64, error) {
	d, _ := r.(*decoder)
	if d == nil {
		d = &decoder{
			reader: r,
			remain: 4,
		}
	}

	limit := d.remain
	size := d.readInt32()

	if d.err != nil {
		return int64(limit - d.remain), d.err
	}

	if size <= 0 {
		*rs = RecordSet{}
		return 4, nil
	}

	if size < (magicByteOffset + 1) {
		return 4, errorf("impossible record set shorter than %d bytes", magicByteOffset+1)
	}

	var version byte
	switch r := d.reader.(type) {
	case bufferedReader:
		b, err := r.Peek(magicByteOffset + 1)
		if err != nil {
			n, _ := r.Discard(len(b))
			return 4 + int64(n), dontExpectEOF(err)
		}
		version = b[magicByteOffset]
	case bytesBuffer:
		version = r.Bytes()[magicByteOffset]
	default:
		b := make([]byte, magicByteOffset+1)
		if n, err := io.ReadFull(d.reader, b); err != nil {
			return 4 + int64(n), dontExpectEOF(err)
		}
		d.remain -= len(b)
		version = b[magicByteOffset]
		// Reconstruct the prefix that we had to read to determine the version
		// of the record set from the magic byte.
		d.reader = io.MultiReader(bytes.NewReader(b), d.reader)
	}

	d.remain = int(size)

	var err error
	switch version {
	case 0, 1:
		err = rs.readFromVersion1(d)
	case 2:
		err = rs.readFromVersion2(d)
	default:
		err = errorf("unsupported message version %d for message of size %d", version, size)
	}

	rn := 4 + (int(size) - d.remain)
	d.discardAll()
	d.remain = limit - rn
	return int64(rn), err
}

func (rs *RecordSet) readFromVersion1(d *decoder) error {
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
	md := decoder{reader: d}

	for !d.done() {
		md.remain = 12
		offset := md.readInt64()
		md.remain = int(md.readInt32())

		if md.remain == 0 {
			break
		}

		crc := uint32(md.readInt32())
		md.setCRC(crc32.IEEETable)
		magicByte := md.readInt8()
		attributes := md.readInt8()
		timestamp := int64(0)

		if magicByte != 0 {
			timestamp = md.readInt64()
		}

		baseAttributes |= attributes
		var compression = Attributes(attributes).Compression()
		var codec compress.Codec
		if compression != 0 {
			if codec = compression.Codec(); codec == nil {
				return errorf("unsupported compression codec: %d", compression)
			}
		}

		var k ByteSequence
		var v ByteSequence

		keyOffset := buffer.Size()
		keyLength := int(md.readInt32())
		hasKey := keyLength >= 0

		if hasKey {
			md.writeTo(buffer, keyLength)
			k = buffer.ref(keyOffset, buffer.Size())
		}

		valueOffset := buffer.Size()
		valueLength := int(md.readInt32())
		hasValue := valueLength >= 0

		if hasValue {
			if codec == nil {
				md.writeTo(buffer, valueLength)
				v = buffer.ref(valueOffset, buffer.Size())
			} else {
				subset := RecordSet{}
				decompressor := codec.NewReader(&md)

				err := subset.readFromVersion1(&decoder{
					reader: decompressor,
					remain: math.MaxInt32,
				})

				decompressor.Close()

				if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
					return err
				}

				unref(k) // is it valid to have a non-nil key on the root message?
				records = append(records, subset.Records...)
			}
		}

		// When a compression algorithm was configured on the attributes, there
		// is no actual value in this loop iterator, instead the decompressed
		// records have been loaded by calling readFromVersion1 recursively and
		// already added to the `records` list.
		if compression == 0 && md.err == nil {
			records = append(records, Record{
				Offset: offset,
				Time:   makeTime(timestamp),
				Key:    k,
				Value:  v,
			})
		}

		// When we reach this point, we should already have consumed all the
		// bytes in the message. If that's not the case for any reason, discard
		// whatever is left so we position the stream properly to read the next
		// message.
		if !md.done() {
			md.discardAll()
		}

		// io.ErrUnexpectedEOF could bubble up here if the kafka broker has
		// truncated the response. We can safely handle this error and assume
		// that we have reached the end of the message set.
		if md.err != nil {
			if errors.Is(md.err, io.ErrUnexpectedEOF) {
				break
			}
			return md.err
		}

		if md.crc32 != crc {
			return errorf("crc32 checksum mismatch (computed=%d found=%d)", md.crc32, crc)
		}
	}

	*rs = RecordSet{
		Version:    1,
		Records:    records,
		Attributes: Attributes(baseAttributes),
	}

	if len(records) != 0 {
		rs.BaseOffset = records[0].Offset
	}

	records = nil
	return nil
}

func (rs *RecordSet) readFromVersion2(d *decoder) error {
	var (
		baseOffset           = d.readInt64()
		batchLength          = d.readInt32()
		partitionLeaderEpoch = d.readInt32()
		magicByte            = d.readInt8()
		crc                  = d.readInt32()
	)

	d.setCRC(crc32.MakeTable(crc32.Castagnoli))

	var (
		attributes      = d.readInt16()
		lastOffsetDelta = d.readInt32()
		firstTimestamp  = d.readInt64()
		maxTimestamp    = d.readInt64()
		producerID      = d.readInt64()
		producerEpoch   = d.readInt16()
		baseSequence    = d.readInt32()
		numRecords      = d.readInt32()
		reader          = io.Reader(d)
	)

	_ = batchLength     // batch ends when we reach `size`
	_ = lastOffsetDelta // this is not useful for reading the records
	_ = maxTimestamp    // not sure what this field is useful for

	if compression := Attributes(attributes).Compression(); compression != 0 {
		codec := compression.Codec()
		if codec == nil {
			return errorf("unsupported compression codec (%d)", compression)
		}
		decompressor := codec.NewReader(reader)
		defer decompressor.Close()
		reader = decompressor
	}

	buffer := newPageBuffer()
	defer buffer.unref()

	_, err := buffer.ReadFrom(reader)
	if err != nil {
		return err
	}
	if d.crc32 != uint32(crc) {
		return errorf("crc32 checksum mismatch (computed=%d found=%d)", d.crc32, uint32(crc))
	}

	recordsLength := buffer.Len()

	d = &decoder{
		reader: buffer,
		remain: recordsLength,
	}

	records := make([]Record, numRecords)

	for i := range records {
		_ = d.readVarInt() // record length (unused)
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

	// Ignore io.ErrUnexpectedEOF which occurs when the broker truncates the
	// response.
	//
	// Note: it's unclear whether kafka 0.11+ still truncates the responses,
	// all attempts I made at constructing a test to trigger a truncation have
	// failed. I kept this code here as a safeguard but it may never execute.
	if d.err != nil && !errors.Is(d.err, io.ErrUnexpectedEOF) {
		for _, r := range records {
			unref(r.Key)
			unref(r.Value)
		}
		return d.err
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

	return nil
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

	// This condition indicates that the output writer received by `WriteTo` was
	// not a *pageBuffer, in which case we need to flush the buffered records
	// data into it.
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
			// In the message format version 1, compression is acheived by
			// compressing the value of a message which recursively contains
			// the representation of the compressed message set.
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

		messageOffset := buffer.Size()
		e.writeInt64(r.Offset)
		e.writeInt32(0) // message size placeholder
		e.writeInt32(0) // crc32 placeholder
		e.setCRC(crc32.IEEETable)
		e.writeInt8(1) // magic byte: version 1
		e.writeInt8(int8(attributes))
		e.writeInt64(timestamp(r.Time))

		if err := e.writeNullBytesFrom(r.Key); err != nil {
			return err
		}

		if err := e.writeNullBytesFrom(r.Value); err != nil {
			return err
		}

		b0 := packUint32(uint32(buffer.Size() - (messageOffset + 12)))
		b1 := packUint32(e.crc32)

		buffer.WriteAt(b0[:], messageOffset+8)
		buffer.WriteAt(b1[:], messageOffset+12)
		e.setCRC(nil)
	}

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

	e := &encoder{writer: buffer}
	e.writeInt64(baseOffset)
	e.writeInt32(0) // placeholder for record batch length
	e.writeInt32(rs.PartitionLeaderEpoch)
	e.writeInt8(2)  // magic byte
	e.writeInt32(0) // placeholder for crc32 checksum
	e.setCRC(crc32.MakeTable(crc32.Castagnoli))
	e.writeInt16(int16(rs.Attributes))
	e.writeInt32(lastOffsetDelta)
	e.writeInt64(firstTimestamp)
	e.writeInt64(maxTimestamp)
	e.writeInt64(rs.ProducerID)
	e.writeInt16(rs.ProducerEpoch)
	e.writeInt32(rs.BaseSequence)
	e.writeInt32(numRecords)

	// The encoder `e` may be assigned to a new value if compression is enabled.
	// Capture the address of its crc32 field which will contain the checksum of
	// the serialized record set after all records have been written.
	var crc = &e.crc32
	var compressor io.WriteCloser

	if compression := rs.Attributes.Compression(); compression != 0 {
		if codec := compression.Codec(); codec != nil {
			compressor = codec.NewWriter(e)
			e = &encoder{writer: compressor}
		}
	}

	for i := range records {
		r := &records[i]
		timestampDelta := timestamp(r.Time) - firstTimestamp
		offsetDelta := r.Offset - baseOffset

		length := 1 + // attributes
			sizeOfVarInt(timestampDelta) +
			sizeOfVarInt(offsetDelta) +
			sizeOfVarBytes(r.Key) +
			sizeOfVarBytes(r.Value) +
			sizeOfVarInt(int64(len(r.Headers)))

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

	b0 := packUint32(uint32(batchLength))
	b1 := packUint32(*crc)

	buffer.WriteAt(b0[:], bufferOffset+8)
	buffer.WriteAt(b1[:], bufferOffset+17)
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

func packUint32(u uint32) (b [4]byte) {
	binary.BigEndian.PutUint32(b[:], u)
	return
}
