package kafka

import (
	"bytes"
	"io"
	"time"

	"github.com/segmentio/kafka-go/protocol"
)

// Header is a key/value pair type representing headers set on records.
type Header = protocol.Header

// Bytes is an interface representing a sequence of bytes. This abstraction
// makes it possible for programs to inject data into produce requests without
// having to load in into an intermediary buffer, or read record keys and values
// from a fetch response directly from internal buffers.
//
// Bytes are not safe to use concurrently from multiple goroutines.
type Bytes = protocol.ByteSequence

// Record is an interface representing a single kafka record.
//
// Record values are not safe to use concurrently from multiple goroutines.
type Record interface {
	// The offset at which the record exists in a topic partition. This value
	// is ignored in produce requests.
	Offset() int64

	// Returns the time of the record. This value may be omitted in produce
	// requests to let kafka set the time when it saves the record.
	Time() time.Time

	// Returns a byte sequence containing the key of this record. The returned
	// sequence may be nil to indicate that the record has no key. If the record
	// is part of a RecordSet, the content of the key must remain valid at least
	// until the record set is closed (or until the key is closed).
	Key() Bytes

	// Returns a byte sequence containing the value of this record. The returned
	// sequence may be nil to indicate that the record has no value. If the
	// record is part of a RecordSet, the content of the value must remain valid
	// at least until the record set is closed (or until the value is closed).
	Value() Bytes

	// Returns the list of headers associated with this record. The returned
	// slice may be reused across calls, the program should use it as an
	// immutable value.
	Headers() []Header
}

// NewRecord constructs a new record from the arguments passed to the function.
func NewRecord(offset int64, time time.Time, key, value []byte, headers ...Header) Record {
	rec := &record{
		offset:      offset,
		time:        time,
		keyIsNull:   key == nil,
		valueIsNull: value == nil,
	}

	rec.key.Reset(key)
	rec.value.Reset(value)

	if len(headers) != 0 {
		rec.headers = make([]Header, len(headers))
		copy(rec.headers, headers)
	}

	return rec
}

type bytesReader struct{ bytes.Reader }

func (r *bytesReader) Close() error {
	r.Reset(nil)
	return nil
}

type record struct {
	offset int64
	time   time.Time

	key     bytesReader
	value   bytesReader
	headers []Header

	keyIsNull   bool
	valueIsNull bool
}

func (rec *record) Offset() int64 { return rec.offset }

func (rec *record) Time() time.Time { return rec.time }

func (rec *record) Key() Bytes {
	if rec.keyIsNull {
		return nil
	}
	return &rec.key
}

func (rec *record) Value() Bytes {
	if rec.valueIsNull {
		return nil
	}
	return &rec.value
}

func (rec *record) Headers() []Header { return rec.headers }

// RecordSet is an interface representing a sequence of records. Record sets are
// used in both produce and fetch requests to represent the sequence of records
// that are sent to or receive from kafka brokers.
//
// RecordSet values are not safe to use concurrently from multiple goroutines.
type RecordSet interface {
	io.Closer
	// Returns the next record in the set, or nil if all records of the sequence
	// have already been returned.
	Next() Record
}

// NewRecordSet constructs a RecordSet which exposes the sequence of records
// passed as arguments.
func NewRecordSet(records ...Record) RecordSet {
	set := &recordSet{records: make([]Record, len(records))}
	copy(set.records, records)
	return set
}

type recordSet struct {
	records []Record
	index   int
}

func (set *recordSet) Close() error {
	set.index = len(set.records)
	return nil
}

func (set *recordSet) Next() Record {
	if set.index >= 0 && set.index < len(set.records) {
		rec := set.records[set.index]
		set.index++
		return rec
	}
	return nil
}
