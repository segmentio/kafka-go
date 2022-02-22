package protocol_test

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

type memoryRecord struct {
	offset  int64
	time    time.Time
	key     []byte
	value   []byte
	headers []protocol.Header
}

func (m *memoryRecord) Record() protocol.Record {
	return protocol.Record{
		Offset:  m.offset,
		Time:    m.time,
		Key:     protocol.NewBytes(m.key),
		Value:   protocol.NewBytes(m.value),
		Headers: m.headers,
	}
}

func now() time.Time {
	return protocol.MakeTime(protocol.Timestamp(time.Now()))
}

func makeRecords(memoryRecords []memoryRecord) []protocol.Record {
	records := make([]protocol.Record, len(memoryRecords))
	for i, m := range memoryRecords {
		records[i] = m.Record()
	}
	return records
}

func TestRecordReader(t *testing.T) {
	now := now()

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
			headers: []protocol.Header{
				{Key: "answer", Value: []byte("42")},
			},
		},
	}

	r1 := protocol.NewRecordReader(makeRecords(records)...)
	r2 := protocol.NewRecordReader(makeRecords(records)...)
	prototest.AssertRecords(t, r1, r2)
}

func TestMultiRecordReader(t *testing.T) {
	now := now()

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
			headers: []protocol.Header{
				{Key: "answer", Value: []byte("42")},
			},
		},
	}

	r1 := protocol.NewRecordReader(makeRecords(records)...)
	r2 := protocol.MultiRecordReader(
		protocol.NewRecordReader(makeRecords(records[:1])...),
		protocol.NewRecordReader(makeRecords(records[1:])...),
	)
	prototest.AssertRecords(t, r1, r2)
}

func TestControlRecord(t *testing.T) {
	now := now()

	records := []protocol.ControlRecord{
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
			Headers: []protocol.Header{
				{Key: "answer", Value: []byte("42")},
			},
		},
	}

	batch := protocol.NewControlBatch(records...)
	found := make([]protocol.ControlRecord, 0, len(records))

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

func TestRecordBatchWriteToReadFrom(t *testing.T) {
	now := now()

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
			headers: []protocol.Header{
				{Key: "answer", Value: []byte("42")},
			},
		},
	}

	original := &protocol.RecordBatch{
		Attributes:           protocol.Snappy,
		PartitionLeaderEpoch: -1,
		BaseOffset:           records[0].offset,
		LastOffsetDelta:      int32(records[2].offset - records[0].offset),
		FirstTimestamp:       records[0].time,
		MaxTimestamp:         records[2].time,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		BaseSequence:         -1,
		NumRecords:           3,
		Records:              protocol.NewRecordReader(makeRecords(records)...),
	}

	buffer := new(bytes.Buffer)
	if _, err := original.WriteTo(buffer); err != nil {
		t.Fatal("writing record batch:", err)
	}

	reloaded := new(protocol.RecordBatch)
	if _, err := reloaded.ReadFrom(buffer); err != nil {
		t.Fatal("reading record batch:", err)
	}

	prototest.AssertRecords(t, reloaded,
		protocol.NewRecordReader(makeRecords(records)...),
	)
	original.Records = nil
	reloaded.Records = nil

	if !reflect.DeepEqual(reloaded, original) {
		t.Errorf("record batches mismatch:\nwant = %+v\ngot  = %+v", original, reloaded)
	}
}
