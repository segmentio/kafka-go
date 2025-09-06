package protocol

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

type memoryRecord struct {
	offset  int64
	time    time.Time
	key     []byte
	value   []byte
	headers []Header
}

func (m *memoryRecord) Record() Record {
	return Record{
		Offset:  m.offset,
		Time:    m.time,
		Key:     NewBytes(m.key),
		Value:   NewBytes(m.value),
		Headers: m.headers,
	}
}

func makeRecords(memoryRecords []memoryRecord) []Record {
	records := make([]Record, len(memoryRecords))
	for i, m := range memoryRecords {
		records[i] = m.Record()
	}
	return records
}

// TestBaseOffset verifies that serializing and then deserializing a RecordSet should preserve
// the offsets correctly by writing the base offset to the buffer.
func TestBaseOffset(t *testing.T) {
	now := time.Now()

	records := []memoryRecord{
		{
			// start the offset at an arbitrary value
			offset: 101,
			time:   now,
			key:    []byte("key-1"),
		},
		{
			offset: 102,
			time:   now.Add(time.Millisecond),
			value:  []byte("value-1"),
		},
		{
			offset: 103,
			time:   now.Add(time.Second),
			key:    []byte("key-3"),
			value:  []byte("value-3"),
			headers: []Header{
				{Key: "answer", Value: []byte("42")},
			},
		},
	}

	recordSet := RecordSet{Version: 2, Records: NewRecordReader(makeRecords(records)...)}

	// Serialize the record set into a buffer
	wr := bytes.NewBuffer(nil)
	if _, err := recordSet.WriteTo(wr); err != nil {
		t.Errorf("failed to serialize RecordSet: %s", err)
	}

	// Deserialize the buffer back into a RecordSet
	var deserialized RecordSet
	if _, err := deserialized.ReadFrom(wr); err != nil {
		t.Errorf("failed to deserialize RecordSet: %s", err)
	}

	for _, expectedRecord := range records {
		record, err := deserialized.Records.ReadRecord()
		if err != nil {
			t.Fatalf("failed to read the next record: %s", err)
		}

		if record.Offset != expectedRecord.offset {
			t.Error("record that was serialized and then deserialized had an incorrect offset")
			t.Errorf("expected offset: %v", expectedRecord.offset)
			t.Errorf("actual offset: %v", record.Offset)
		}
	}
}

func TestRecordReader(t *testing.T) {
	now := time.Now()

	records := []memoryRecord{
		{
			offset: 1,
			time:   now,
			key:    []byte("key-1"),
		},
		{
			offset: 2,
			time:   now.Add(time.Millisecond),
			value:  []byte("value-1"),
		},
		{
			offset: 3,
			time:   now.Add(time.Second),
			key:    []byte("key-3"),
			value:  []byte("value-3"),
			headers: []Header{
				{Key: "answer", Value: []byte("42")},
			},
		},
	}

	r1 := NewRecordReader(makeRecords(records)...)
	r2 := NewRecordReader(makeRecords(records)...)
	assertRecords(t, r1, r2)
}

func TestMultiRecordReader(t *testing.T) {
	now := time.Now()

	records := []memoryRecord{
		{
			offset: 1,
			time:   now,
			key:    []byte("key-1"),
		},
		{
			offset: 2,
			time:   now.Add(time.Millisecond),
			value:  []byte("value-1"),
		},
		{
			offset: 3,
			time:   now.Add(time.Second),
			key:    []byte("key-3"),
			value:  []byte("value-3"),
			headers: []Header{
				{Key: "answer", Value: []byte("42")},
			},
		},
	}

	r1 := NewRecordReader(makeRecords(records)...)
	r2 := MultiRecordReader(
		NewRecordReader(makeRecords(records[:1])...),
		NewRecordReader(makeRecords(records[1:])...),
	)
	assertRecords(t, r1, r2)
}

func TestControlRecord(t *testing.T) {
	now := time.Now()

	records := []ControlRecord{
		{
			Offset:  1,
			Time:    now,
			Version: 2,
			Type:    3,
		},
		{
			Offset:  2,
			Time:    now.Add(time.Second),
			Version: 4,
			Type:    5,
			Data:    []byte("Hello World!"),
			Headers: []Header{
				{Key: "answer", Value: []byte("42")},
			},
		},
	}

	batch := NewControlBatch(records...)
	found := make([]ControlRecord, 0, len(records))

	for {
		r, err := batch.ReadControlRecord()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			break
		}
		found = append(found, *r)
	}

	if !reflect.DeepEqual(records, found) {
		t.Error("control records mismatch")
	}
}

func assertRecords(t *testing.T, r1, r2 RecordReader) {
	t.Helper()

	for {
		rec1, err1 := r1.ReadRecord()
		rec2, err2 := r2.ReadRecord()

		if err1 != nil || err2 != nil {
			if !errors.Is(err1, err2) {
				t.Error("errors mismatch:")
				t.Log("expected:", err2)
				t.Log("found:   ", err1)
			}
			return
		}

		if !equalRecords(rec1, rec2) {
			t.Error("records mismatch:")
			t.Logf("expected: %+v", rec2)
			t.Logf("found:    %+v", rec1)
		}
	}
}

func equalRecords(r1, r2 *Record) bool {
	if r1.Offset != r2.Offset {
		return false
	}

	if !r1.Time.Equal(r2.Time) {
		return false
	}

	k1 := readAll(r1.Key)
	k2 := readAll(r2.Key)

	if !reflect.DeepEqual(k1, k2) {
		return false
	}

	v1 := readAll(r1.Value)
	v2 := readAll(r2.Value)

	if !reflect.DeepEqual(v1, v2) {
		return false
	}

	return reflect.DeepEqual(r1.Headers, r2.Headers)
}

func readAll(bytes Bytes) []byte {
	if bytes != nil {
		defer bytes.Close()
	}
	b, err := ReadAll(bytes)
	if err != nil {
		panic(err)
	}
	return b
}
