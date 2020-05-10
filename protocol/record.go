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

// Record is an interface representing a single kafka record.
//
// Record values are not safe to use concurrently from multiple goroutines.
type Record struct {
	// The offset at which the record exists in a topic partition. This value
	// is ignored in produce requests.
	Offset int64

	// Returns the time of the record. This value may be omitted in produce
	// requests to let kafka set the time when it saves the record.
	Time time.Time

	// Returns a byte sequence containing the key of this record. The returned
	// sequence may be nil to indicate that the record has no key. If the record
	// is part of a RecordSet, the content of the key must remain valid at least
	// until the record set is closed (or until the key is closed).
	Key Bytes

	// Returns a byte sequence containing the value of this record. The returned
	// sequence may be nil to indicate that the record has no value. If the
	// record is part of a RecordSet, the content of the value must remain valid
	// at least until the record set is closed (or until the value is closed).
	Value Bytes

	// Returns the list of headers associated with this record. The returned
	// slice may be reused across calls, the program should use it as an
	// immutable value.
	Headers []Header
}

// RecordBatch is an interface representing a sequence of records. Record sets
// are used in both produce and fetch requests to represent the sequence of
// records that are sent to or receive from kafka brokers.
//
// RecordSet values are not safe to use concurrently from multiple goroutines.
type RecordBatch interface {
	io.Closer
	// Returns the next record in the set, or io.EOF if the end of the sequence
	// has been reached.
	//
	// The returned Record is guaranteed to be valid until the next call to
	// ReadRecord. If the program needs to retain the Record value it must make
	// a copy.
	ReadRecord() (*Record, error)
}

// NewRecordBatch constructs a reader exposing the records passed as arguments.
func NewRecordBatch(records ...Record) RecordBatch {
	r := &recordReader{records: make([]Record, len(records))}
	copy(r.records, records)
	return r
}

func forEachRecord(r RecordBatch, f func(int, *Record) error) error {
	defer r.Close()

	for i := 0; ; i++ {
		rec, err := r.ReadRecord()

		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return err
		}

		if err := handleRecord(i, rec, f); err != nil {
			return err
		}
	}
}

func handleRecord(i int, r *Record, f func(int, *Record) error) error {
	if r.Key != nil {
		defer r.Key.Close()
	}
	if r.Value != nil {
		defer r.Value.Close()
	}
	if r.Offset == 0 {
		r.Offset = int64(i)
	}
	return f(i, r)
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
	// A reader exposing the sequence of records.
	Records RecordBatch
	// Inlined reader to avoid the separate heap allocation that occurs in the
	// common case.
	records optimizedRecordBatch
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

		var k Bytes
		var v Bytes

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
				records = append(records, subset.Records.(*recordReader).records...)
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
		Attributes: Attributes(baseAttributes),
		Records: &recordReader{
			records: records,
		},
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

	records := make([]optimizedRecord, numRecords)

	for i := range records {
		r := &records[i]
		_ = d.readVarInt() // record length (unused)
		_ = d.readInt8()   // record attributes (unused)
		timestampDelta := d.readVarInt()
		offsetDelta := d.readVarInt()

		r.offset = baseOffset + offsetDelta
		r.timestamp = firstTimestamp + timestampDelta

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

		if numHeaders := d.readVarInt(); numHeaders > 0 {
			r.headers = make([]Header, numHeaders)

			for i := range r.headers {
				r.headers[i] = Header{
					Key:   d.readCompactString(),
					Value: d.readCompactBytes(),
				}
			}
		}

		if d.err != nil {
			records = records[:i]
			break
		}

		if keyLength >= 0 {
			buffer.refTo(&r.keyRef, keyOffset, keyOffset+keyLength)
		}

		if valueLength >= 0 {
			buffer.refTo(&r.valueRef, valueOffset, valueOffset+valueLength)
		}
	}

	// Ignore io.ErrUnexpectedEOF which occurs when the broker truncates the
	// response.
	//
	// Note: it's unclear whether kafka 0.11+ still truncates the responses,
	// all attempts I made at constructing a test to trigger a truncation have
	// failed. I kept this code here as a safeguard but it may never execute.
	if d.err != nil && !errors.Is(d.err, io.ErrUnexpectedEOF) {
		for i := range records {
			r := &records[i]
			r.unref()
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
	}

	rs.Records = &rs.records
	rs.records.records = records
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

	if records == nil {
		return nil
	}

	if compression := attributes.Compression(); compression != 0 {
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

			records = &recordReader{
				records: []Record{{
					Value: compressed,
				}},
			}
		}
	}

	e := encoder{writer: buffer}

	return forEachRecord(records, func(i int, r *Record) error {
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
		return nil
	})
}

func (rs *RecordSet) writeToVersion2(buffer *pageBuffer, bufferOffset int64) error {
	records := rs.Records
	baseOffset := rs.BaseOffset
	numRecords := int32(0)

	if records == nil {
		return nil
	}

	e := &encoder{writer: buffer}
	e.writeInt64(baseOffset)              //                                     |  0 +8
	e.writeInt32(0)                       // placeholder for record batch length |  8 +4
	e.writeInt32(rs.PartitionLeaderEpoch) //                                     | 12 +3
	e.writeInt8(2)                        // magic byte                          | 16 +1
	e.writeInt32(0)                       // placeholder for crc32 checksum      | 17 +4
	e.writeInt16(int16(rs.Attributes))    //                                     | 21 +2
	e.writeInt32(0)                       // placeholder for lastOffsetDelta     | 23 +4
	e.writeInt64(0)                       // placeholder for firstTimestamp      | 27 +8
	e.writeInt64(0)                       // placeholder for maxTimestamp        | 35 +8
	e.writeInt64(rs.ProducerID)           //                                     | 43 +8
	e.writeInt16(rs.ProducerEpoch)        //                                     | 51 +2
	e.writeInt32(rs.BaseSequence)         //                                     | 53 +4
	e.writeInt32(0)                       // placeholder for numRecords          | 57 +4

	var compressor io.WriteCloser
	if compression := rs.Attributes.Compression(); compression != 0 {
		if codec := compression.Codec(); codec != nil {
			compressor = codec.NewWriter(buffer)
			e.writer = compressor
		}
	}

	lastOffsetDelta := int32(0)
	firstTimestamp := int64(0)
	maxTimestamp := int64(0)

	err := forEachRecord(records, func(i int, r *Record) error {
		maxTimestamp = timestamp(r.Time)
		if i == 0 {
			firstTimestamp = maxTimestamp
		}

		timestampDelta := maxTimestamp - firstTimestamp
		offsetDelta := r.Offset - baseOffset
		lastOffsetDelta = int32(offsetDelta)

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

		numRecords++
		return nil
	})

	if err != nil {
		return err
	}

	if compressor != nil {
		if err := compressor.Close(); err != nil {
			return err
		}
	}

	b2 := packUint32(uint32(lastOffsetDelta))
	b3 := packUint64(uint64(firstTimestamp))
	b4 := packUint64(uint64(maxTimestamp))
	b5 := packUint32(uint32(numRecords))

	buffer.WriteAt(b2[:], bufferOffset+23)
	buffer.WriteAt(b3[:], bufferOffset+27)
	buffer.WriteAt(b4[:], bufferOffset+35)
	buffer.WriteAt(b5[:], bufferOffset+57)

	totalLength := buffer.Size() - bufferOffset
	checksum := uint32(0)
	crcTable := crc32.MakeTable(crc32.Castagnoli)

	buffer.pages.scan(bufferOffset+21, bufferOffset+totalLength, func(chunk []byte) bool {
		checksum = crc32.Update(checksum, crcTable, chunk)
		return true
	})

	b0 := packUint32(uint32(totalLength - 12))
	b1 := packUint32(checksum)

	buffer.WriteAt(b0[:], bufferOffset+8)
	buffer.WriteAt(b1[:], bufferOffset+17)
	return nil
}

func makeBytes(ref *pageRef) Bytes {
	if ref.buffer == nil {
		return nil
	}
	return ref
}

func makeTime(t int64) time.Time {
	return time.Unix(t/1000, (t%1000)*int64(time.Millisecond))
}

func timestamp(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano() / int64(time.Millisecond)
}

func packUint32(u uint32) (b [4]byte) {
	binary.BigEndian.PutUint32(b[:], u)
	return
}

func packUint64(u uint64) (b [8]byte) {
	binary.BigEndian.PutUint64(b[:], u)
	return
}

type recordReader struct {
	records []Record
	index   int
}

func (r *recordReader) Close() error {
	for _, rec := range r.records[r.index:] {
		if rec.Key != nil {
			rec.Key.Close()
		}
		if rec.Value != nil {
			rec.Value.Close()
		}
	}
	r.index = len(r.records)
	return nil
}

func (r *recordReader) Reset() {
	r.index = 0

	for i := range r.records {
		r := &r.records[i]
		reset(r.Key)
		reset(r.Value)
	}
}

func (r *recordReader) ReadRecord() (*Record, error) {
	if i := r.index; i >= 0 && i < len(r.records) {
		r.index++
		return &r.records[i], nil
	}
	return nil, io.EOF
}

type optimizedRecordBatch struct {
	records []optimizedRecord
	index   int
	buffer  Record
}

func (r *optimizedRecordBatch) Close() error {
	for i := range r.records {
		r.records[i].unref()
	}
	return nil
}

func (r *optimizedRecordBatch) ReadRecord() (*Record, error) {
	if i := r.index; i >= 0 && i < len(r.records) {
		rec := &r.records[i]
		r.index++
		r.buffer = Record{
			Offset:  rec.offset,
			Time:    rec.time(),
			Key:     rec.key(),
			Value:   rec.value(),
			Headers: rec.headers,
		}
		return &r.buffer, nil
	}
	return nil, io.EOF
}

type optimizedRecord struct {
	offset    int64
	timestamp int64
	keyRef    pageRef
	valueRef  pageRef
	headers   []Header
}

func (r *optimizedRecord) unref() {
	r.keyRef.unref()
	r.valueRef.unref()
}

func (r *optimizedRecord) time() time.Time {
	return makeTime(r.timestamp)
}

func (r *optimizedRecord) key() Bytes {
	return makeBytes(&r.keyRef)
}

func (r *optimizedRecord) value() Bytes {
	return makeBytes(&r.valueRef)
}
