package prototest

import (
	"io"
	"reflect"
	"testing"

	"github.com/segmentio/kafka-go/protocol"
)

func AssertRecords(t *testing.T, r1, r2 protocol.RecordReader) (ok bool) {
	t.Helper()
	ok = true

	n1 := r1.Len()
	n2 := r2.Len()
	if n1 != n2 {
		t.Errorf("number of records mismatch: r1=%d r2=%d", n1, n2)
	}

	defer func() {
		if err := r1.Close(); err != nil {
			t.Errorf("closing first record reader: %v", err)
			ok = false
		}
		if err := r2.Close(); err != nil {
			t.Errorf("closing second record reader: %v", err)
			ok = false
		}
	}()

	i := 0
	for {
		rec1, err1 := r1.ReadRecord()
		rec2, err2 := r2.ReadRecord()

		if err1 != nil || err2 != nil {
			if err1 != err2 {
				t.Errorf("errors mismatch at index %d:", i)
				t.Log("expected:", err2)
				t.Log("found:   ", err1)
				ok = false
			}
			return ok
		}

		if eq := EqualRecords(rec1, rec2); eq != EqualRecordsOK {
			t.Errorf("records mismatch at index %d: %s", i, eq)
			t.Logf("expected: %+v", rec2)
			t.Logf("found:    %+v", rec1)
			ok = false
			return ok
		}

		i++
	}

	_, err1 := r1.ReadRecord()
	_, err2 := r2.ReadRecord()
	if err1 != io.EOF || err2 != io.EOF {
		t.Errorf("unexpected error found after reading all records: %v/%v", err1, err2)
		ok = false
	}

	if n := r1.Len(); n != 0 {
		t.Errorf("wrong number of records remaining after reading all records: r1=%d", n)
	}
	if n := r2.Len(); n != 0 {
		t.Errorf("wrong number of records remaining after reading all records: r2=%d", n)
	}

	return ok
}

type EqualRecordsResult int

const (
	EqualRecordsOK EqualRecordsResult = iota
	EqualRecordsMismatchOffset
	EqualRecordsMismatchTime
	EqualRecordsMismatchKey
	EqualRecordsMismatchValue
	EqualRecordsMismatchHeaders
)

func (eq EqualRecordsResult) String() string {
	switch eq {
	case EqualRecordsMismatchOffset:
		return "offset mismatch"
	case EqualRecordsMismatchTime:
		return "time mismatch"
	case EqualRecordsMismatchKey:
		return "key mismatch"
	case EqualRecordsMismatchValue:
		return "value mismatch"
	case EqualRecordsMismatchHeaders:
		return "headers mismatch"
	default:
		return "OK"
	}
}

func EqualRecords(r1, r2 *protocol.Record) EqualRecordsResult {
	if r1.Offset != r2.Offset {
		return EqualRecordsMismatchOffset
	}

	if !r1.Time.Equal(r2.Time) {
		return EqualRecordsMismatchTime
	}

	k1 := ReadAll(r1.Key)
	k2 := ReadAll(r2.Key)

	if !reflect.DeepEqual(k1, k2) {
		return EqualRecordsMismatchKey
	}

	v1 := ReadAll(r1.Value)
	v2 := ReadAll(r2.Value)

	if !reflect.DeepEqual(v1, v2) {
		return EqualRecordsMismatchValue
	}

	if !reflect.DeepEqual(r1.Headers, r2.Headers) {
		return EqualRecordsMismatchHeaders
	}

	return EqualRecordsOK
}

func ReadAll(bytes protocol.Bytes) []byte {
	b, err := protocol.ReadAll(bytes)
	if err != nil {
		panic(err)
	}
	return b
}
