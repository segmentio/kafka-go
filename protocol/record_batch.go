package protocol

import (
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

// RecordReader is an interface representing a sequence of records. Record sets
// are used in both produce and fetch requests to represent the sequence of
// records that are sent to or receive from kafka brokers.
//
// When the program is done reading the records, it must call the Close method
// to release the resources held by the reader.
type RecordReader interface {
	io.Closer
	// Returns the number of records remaining to be read from the sequence.
	Len() int
	// Returns the next record in the set, or io.EOF if the end of the sequence
	// has been reached.
	//
	// The returned Record is guaranteed to be valid until the next call to
	// ReadRecord or Close. If the program needs to retain the Record value it
	// must make a copy of it, including copying the record key and value.
	ReadRecord() (*Record, error)
}

// NewRecordReader constructs a reader exposing the records passed as arguments.
func NewRecordReader(records ...Record) RecordReader {
	switch len(records) {
	case 0:
		return emptyRecordReader{}
	default:
		r := &recordReader{records: make([]Record, len(records))}
		copy(r.records, records)
		return r
	}
}

// MultiRecordReader merges multiple record batches into one.
func MultiRecordReader(batches ...RecordReader) RecordReader {
	switch len(batches) {
	case 0:
		return emptyRecordReader{}
	case 1:
		return batches[0]
	default:
		m := &multiRecordReader{batches: make([]RecordReader, len(batches))}
		copy(m.batches, batches)
		return m
	}
}

func forEachRecord(r RecordReader, f func(int, *Record) error) error {
	defer r.Close()

	for i := 0; ; i++ {
		rec, err := r.ReadRecord()

		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return err
		}

		if err := f(i, rec); err != nil {
			return err
		}
	}

	return r.Close()
}

type recordReader struct {
	records []Record
	index   int
}

func (r *recordReader) Close() error {
	r.records, r.index = nil, 0
	return nil
}

func (r *recordReader) Len() int {
	return len(r.records) - r.index
}

func (r *recordReader) ReadRecord() (*Record, error) {
	if i := r.index; i >= 0 && i < len(r.records) {
		r.index++
		return &r.records[i], nil
	}
	return nil, io.EOF
}

type multiRecordReader struct {
	batches []RecordReader
	index   int
}

func (m *multiRecordReader) Close() error {
	for _, r := range m.batches {
		r.Close() // TODO: should we report errors here?
	}
	m.batches, m.index = nil, 0
	return nil
}

func (m *multiRecordReader) Len() (n int) {
	for _, r := range m.batches {
		n += r.Len()
	}
	return n
}

func (m *multiRecordReader) ReadRecord() (*Record, error) {
	for {
		if m.index == len(m.batches) {
			return nil, io.EOF
		}
		r, err := m.batches[m.index].ReadRecord()
		if err == nil {
			return r, nil
		}
		if !errors.Is(err, io.EOF) {
			return nil, err
		}
		m.index++
	}
}

func concatRecordReader(head RecordReader, tail RecordReader) RecordReader {
	if head == nil {
		return tail
	}
	if m, _ := head.(*multiRecordReader); m != nil {
		m.batches = append(m.batches, tail)
		return m
	}
	return MultiRecordReader(head, tail)
}

// optimizedRecordReader is an implementation of a RecordReader which exposes a
// sequence
type optimizedRecordReader struct {
	records []optimizedRecord
	index   int
	buffer  Record
	headers [][]Header
}

func (r *optimizedRecordReader) Close() error {
	for i := range r.records {
		r.records[i].unref()
	}
	r.records = nil
	r.index = 0
	r.buffer = Record{}
	r.headers = nil
	return nil
}

func (r *optimizedRecordReader) Len() int {
	return len(r.records) - r.index
}

func (r *optimizedRecordReader) ReadRecord() (*Record, error) {
	if i := r.index; i >= 0 && i < len(r.records) {
		rec := &r.records[i]
		r.index++
		r.buffer = Record{
			Offset: rec.offset,
			Time:   rec.time(),
			Key:    rec.key(),
			Value:  rec.value(),
		}
		if i < len(r.headers) {
			r.buffer.Headers = r.headers[i]
		}
		return &r.buffer, nil
	}
	return nil, io.EOF
}

type optimizedRecord struct {
	offset    int64
	timestamp int64
	keyRef    *pageRef
	valueRef  *pageRef
}

func (r *optimizedRecord) unref() {
	if r.keyRef != nil {
		r.keyRef.unref()
		r.keyRef = nil
	}
	if r.valueRef != nil {
		r.valueRef.unref()
		r.valueRef = nil
	}
}

func (r *optimizedRecord) time() time.Time {
	return MakeTime(r.timestamp)
}

func (r *optimizedRecord) key() Bytes {
	return makeBytes(r.keyRef)
}

func (r *optimizedRecord) value() Bytes {
	return makeBytes(r.valueRef)
}

func makeBytes(ref *pageRef) Bytes {
	if ref == nil {
		return nil
	}
	return ref
}

type emptyRecordReader struct{}

func (emptyRecordReader) Close() error                 { return nil }
func (emptyRecordReader) Len() int                     { return 0 }
func (emptyRecordReader) ReadRecord() (*Record, error) { return nil, io.EOF }

// ControlRecord represents a record read from a control batch.
type ControlRecord struct {
	Offset  int64
	Time    time.Time
	Version int16
	Type    int16
	Data    []byte
	Headers []Header
}

func ReadControlRecord(r *Record) (*ControlRecord, error) {
	k, err := ReadAll(r.Key)
	if err != nil {
		return nil, err
	}
	if k == nil {
		return nil, Error("invalid control record with nil key")
	}
	if len(k) != 4 {
		return nil, Errorf("invalid control record with key of size %d", len(k))
	}

	v, err := ReadAll(r.Value)
	if err != nil {
		return nil, err
	}

	c := &ControlRecord{
		Offset:  r.Offset,
		Time:    r.Time,
		Version: readInt16(k[:2]),
		Type:    readInt16(k[2:]),
		Data:    v,
		Headers: r.Headers,
	}

	return c, nil
}

func (cr *ControlRecord) Key() Bytes {
	k := make([]byte, 4)
	writeInt16(k[:2], cr.Version)
	writeInt16(k[2:], cr.Type)
	return NewBytes(k)
}

func (cr *ControlRecord) Value() Bytes {
	return NewBytes(cr.Data)
}

func (cr *ControlRecord) Record() Record {
	return Record{
		Offset:  cr.Offset,
		Time:    cr.Time,
		Key:     cr.Key(),
		Value:   cr.Value(),
		Headers: cr.Headers,
	}
}

// ControlBatch is an implementation of the RecordReader interface representing
// control batches returned by kafka brokers.
type ControlBatch RecordBatch

// NewControlBatch constructs a control batch from the list of records passed as
// arguments.
func NewControlBatch(records ...ControlRecord) *ControlBatch {
	rawRecords := make([]Record, len(records))
	for i, cr := range records {
		rawRecords[i] = cr.Record()
	}
	return &ControlBatch{
		Records: NewRecordReader(rawRecords...),
	}
}

func (c *ControlBatch) Close() error {
	return c.Records.Close()
}

func (c *ControlBatch) Len() int {
	return c.Records.Len()
}

func (c *ControlBatch) ReadRecord() (*Record, error) {
	return c.Records.ReadRecord()
}

func (c *ControlBatch) ReadControlRecord() (*ControlRecord, error) {
	r, err := c.ReadRecord()
	if err != nil {
		return nil, err
	}
	return ReadControlRecord(r)
}

func (c *ControlBatch) Version() int {
	return 2
}

// RecordBatch is an implementation of the RecordReader interface representing
// regular record batches (v2).
type RecordBatch struct {
	Attributes           Attributes
	PartitionLeaderEpoch int32
	BaseOffset           int64
	LastOffsetDelta      int32
	FirstTimestamp       time.Time
	MaxTimestamp         time.Time
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	NumRecords           int32
	Records              RecordReader
}

func (r *RecordBatch) Close() error {
	return r.Records.Close()
}

func (r *RecordBatch) Len() int {
	return r.Records.Len()
}

func (r *RecordBatch) ReadRecord() (*Record, error) {
	return r.Records.ReadRecord()
}

func (r *RecordBatch) Version() int {
	return 2
}

func (r *RecordBatch) ReadFrom(in io.Reader) (int64, error) {
	dec := decoder{reader: in, remain: math.MaxInt32}
	tmp := RecordSet{Version: 2}
	n, err := tmp.readFromVersion2(&dec)
	if err != nil {
		return 0, err
	}
	// TODO: we can remove the temporary allocation of records into the
	// protocol.RecordSet.Records value by revisiting the internal code
	// structure (e.g. making readFromVersion2 invoke RecordBatch.ReadFrom?).
	switch records := tmp.Records.(type) {
	case *ControlBatch:
		*r = *(*RecordBatch)(records)
	case *RecordBatch:
		*r = *records
	default:
		return n, fmt.Errorf("protocol.ReadRecordBatch: got unsupported record batch of type %T", r)
	}
	return n, nil
}

func (r *RecordBatch) WriteTo(out io.Writer) (int64, error) {
	buf := newPageBuffer()
	defer buf.unref()
	tmp := RecordSet{Version: 2, Records: r, Attributes: r.Attributes}
	err := tmp.writeToVersion2(buf, 0)
	if err != nil {
		return 0, err
	}
	return buf.WriteTo(out)
}

// MessageSet is an implementation of the RecordReader interface representing
// regular message sets (v1).
type MessageSet struct {
	Attributes Attributes
	BaseOffset int64
	Records    RecordReader
}

func (m *MessageSet) Close() error {
	return m.Records.Close()
}

func (m *MessageSet) Len() int {
	return m.Records.Len()
}

func (m *MessageSet) ReadRecord() (*Record, error) {
	return m.Records.ReadRecord()
}

func (m *MessageSet) Version() int {
	return 1
}

// RecordStream is an implementation of the RecordReader interface which
// combines multiple underlying RecordReader and only expose records that
// are not from control batches.
type RecordStream struct {
	Records []RecordReader
	index   int
}

func (s *RecordStream) Close() error {
	for _, r := range s.Records {
		r.Close()
	}
	s.index = len(s.Records)
	return nil
}

func (s *RecordStream) Len() int {
	n := 0
	for _, r := range s.Records[s.index:] {
		n += r.Len()
	}
	return n
}

func (s *RecordStream) ReadRecord() (*Record, error) {
	for {
		if s.index < 0 || s.index >= len(s.Records) {
			return nil, io.EOF
		}

		if _, isControl := s.Records[s.index].(*ControlBatch); isControl {
			s.index++
			continue
		}

		r, err := s.Records[s.index].ReadRecord()
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.index++
				continue
			}
		}

		return r, err
	}
}

type compressedRecordReader struct {
	records        *optimizedRecordReader
	buffer         *pageBuffer
	decoder        decoder
	attributes     Attributes
	baseOffset     int64
	firstTimestamp int64
	numRecords     int32
	numRecordsRead int32
}

func (r *compressedRecordReader) release() {
	if r.buffer != nil {
		r.buffer.unref()
		r.buffer = nil
	}
}

func (r *compressedRecordReader) Close() (err error) {
	r.release()
	if r.records != nil {
		err = r.records.Close()
		r.records = nil
	}
	r.numRecordsRead = r.numRecords
	return err
}

func (r *compressedRecordReader) Len() int {
	return int(r.numRecords - r.numRecordsRead)
}

func (r *compressedRecordReader) ReadRecord() (*Record, error) {
	if r.buffer != nil {
		defer r.release()
		var err error
		r.records, err = readRecords(r.buffer, &r.decoder, r.attributes, r.baseOffset, r.firstTimestamp, r.numRecords)
		if err != nil {
			return nil, err
		}
	}
	if r.records == nil {
		return nil, io.EOF
	}
	rec, err := r.records.ReadRecord()
	if err == nil {
		r.numRecordsRead++
	}
	return rec, err
}

func (r *compressedRecordReader) Read(b []byte) (int, error) {
	if r.records != nil {
		return 0, errCompressedRecordReaderUsage
	}
	if r.buffer == nil {
		return 0, io.EOF
	}
	n, err := r.buffer.Read(b)
	if err != nil {
		r.release()
		r.numRecordsRead = r.numRecords
	}
	return n, err
}

func (r *compressedRecordReader) WriteTo(w io.Writer) (int64, error) {
	if r.records != nil {
		return 0, errCompressedRecordReaderUsage
	}
	if r.buffer == nil {
		return 0, nil
	}
	defer func() {
		r.release()
		r.numRecordsRead = r.numRecords
	}()
	return r.buffer.WriteTo(w)
}

var (
	errCompressedRecordReaderUsage = errors.New("compressed record reader cannot be both used a kafka.RecordReader and an io.WriterTo")

	_ io.Reader   = (*compressedRecordReader)(nil)
	_ io.WriterTo = (*compressedRecordReader)(nil)

	_ RecordReader = (*MessageSet)(nil)
	_ RecordReader = (*RecordBatch)(nil)
	_ RecordReader = (*ControlBatch)(nil)
)
