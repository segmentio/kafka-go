package prototest

import (
	"errors"
	"io"
	"reflect"
	"time"

	"github.com/apoorvag-mav/kafka-go/protocol"
)

var (
	recordReader = reflect.TypeOf((*protocol.RecordReader)(nil)).Elem()
)

func closeMessage(m protocol.Message) {
	forEachField(reflect.ValueOf(m), func(v reflect.Value) {
		if v.Type().Implements(recordReader) {
			rr := v.Interface().(protocol.RecordReader)
			for {
				r, err := rr.ReadRecord()
				if err != nil {
					break
				}
				if r.Key != nil {
					r.Key.Close()
				}
				if r.Value != nil {
					r.Value.Close()
				}
			}
		}
	})
}

func load(v interface{}) (reset func()) {
	return loadValue(reflect.ValueOf(v))
}

func loadValue(v reflect.Value) (reset func()) {
	resets := []func(){}

	forEachField(v, func(f reflect.Value) {
		switch x := f.Interface().(type) {
		case protocol.RecordReader:
			records := loadRecords(x)
			resetFunc := func() {
				f.Set(reflect.ValueOf(protocol.NewRecordReader(makeRecords(records)...)))
			}
			resetFunc()
			resets = append(resets, resetFunc)
		}
	})

	return func() {
		for _, f := range resets {
			f()
		}
	}
}

func forEachField(v reflect.Value, do func(reflect.Value)) {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Slice:
		for i, n := 0, v.Len(); i < n; i++ {
			forEachField(v.Index(i), do)
		}

	case reflect.Struct:
		for i, n := 0, v.NumField(); i < n; i++ {
			forEachField(v.Field(i), do)
		}

	default:
		do(v)
	}
}

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

func makeRecords(memoryRecords []memoryRecord) []protocol.Record {
	records := make([]protocol.Record, len(memoryRecords))
	for i, m := range memoryRecords {
		records[i] = m.Record()
	}
	return records
}

func loadRecords(r protocol.RecordReader) []memoryRecord {
	records := []memoryRecord{}

	for {
		rec, err := r.ReadRecord()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return records
			}
			panic(err)
		}
		records = append(records, memoryRecord{
			offset:  rec.Offset,
			time:    rec.Time,
			key:     readAll(rec.Key),
			value:   readAll(rec.Value),
			headers: rec.Headers,
		})
	}
}

func readAll(bytes protocol.Bytes) []byte {
	if bytes != nil {
		defer bytes.Close()
	}
	b, err := protocol.ReadAll(bytes)
	if err != nil {
		panic(err)
	}
	return b
}
