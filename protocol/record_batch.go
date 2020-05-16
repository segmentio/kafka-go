package protocol

import (
	"errors"
	"io"
	"time"
)

// RecordBatch is an interface representing a sequence of records. Record sets
// are used in both produce and fetch requests to represent the sequence of
// records that are sent to or receive from kafka brokers.
//
// RecordSet values are not safe to use concurrently from multiple goroutines.
type RecordBatch interface {
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
	switch len(records) {
	case 0:
		return emptyRecordBatch{}
	default:
		r := &recordBatch{records: make([]Record, len(records))}
		copy(r.records, records)
		return r
	}
}

// MultiRecordBatch merges multiple record batches into one.
func MultiRecordBatch(batches ...RecordBatch) RecordBatch {
	switch len(batches) {
	case 0:
		return emptyRecordBatch{}
	case 1:
		return batches[0]
	default:
		m := &multiRecordBatch{batches: make([]RecordBatch, len(batches))}
		copy(m.batches, batches)
		return m
	}
}

// CloseRecordBatch is a helper function to close record batches that have a
// `Close() error` method.
func CloseRecordBatch(rb RecordBatch) error {
	if r, _ := rb.(io.Closer); r != nil {
		return r.Close()
	}
	return nil
}

// ResetRecordBatch is a helper function to reset record batches that have a
// `Reset()` method.
func ResetRecordBatch(rb RecordBatch) {
	if r, _ := rb.(interface{ Reset() }); r != nil {
		r.Reset()
	}
}

func forEachRecord(r RecordBatch, f func(int, *Record) error) error {
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

type recordBatch struct {
	records []Record
	index   int
}

func (r *recordBatch) Close() error {
	for i := range r.records {
		closeBytes(r.records[i].Key)
		closeBytes(r.records[i].Value)
	}
	return nil
}

func (r *recordBatch) Reset() {
	r.index = 0

	for i := range r.records {
		r := &r.records[i]
		resetBytes(r.Key)
		resetBytes(r.Value)
	}
}

func (r *recordBatch) ReadRecord() (*Record, error) {
	if i := r.index; i >= 0 && i < len(r.records) {
		r.index++
		return &r.records[i], nil
	}
	return nil, io.EOF
}

type emptyRecordBatch struct{}

func (emptyRecordBatch) ReadRecord() (*Record, error) { return nil, io.EOF }

type multiRecordBatch struct {
	batches []RecordBatch
	index   int
}

func (m *multiRecordBatch) Close() error {
	for _, b := range m.batches {
		CloseRecordBatch(b)
	}
	m.index = len(m.batches)
	return nil
}

func (m *multiRecordBatch) Reset() {
	for _, b := range m.batches[:m.index] {
		ResetRecordBatch(b)
	}
	m.index = 0
}

func (m *multiRecordBatch) ReadRecord() (*Record, error) {
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

func concatRecordBatch(head RecordBatch, tail RecordBatch) RecordBatch {
	if head == nil {
		return tail
	}
	if m, _ := head.(*multiRecordBatch); m != nil {
		m.batches = append(m.batches, tail)
		return m
	}
	return MultiRecordBatch(head, tail)
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
