package prototest

import (
	"reflect"
	"testing"

	"github.com/segmentio/kafka-go/protocol"
)

func AssertRecords(t *testing.T, r1, r2 protocol.RecordReader) {
	t.Helper()

	defer func() {
		if err := r1.Close(); err != nil {
			t.Errorf("closing first record reader: %v", err)
		}
		if err := r2.Close(); err != nil {
			t.Errorf("closing second record reader: %v", err)
		}
	}()

	for {
		rec1, err1 := r1.ReadRecord()
		rec2, err2 := r2.ReadRecord()

		if err1 != nil || err2 != nil {
			if err1 != err2 {
				t.Error("errors mismatch:")
				t.Log("expected:", err2)
				t.Log("found:   ", err1)
			}
			return
		}

		if !EqualRecords(rec1, rec2) {
			t.Error("records mismatch:")
			t.Logf("expected: %+v", rec2)
			t.Logf("found:    %+v", rec1)
		}
	}
}

func EqualRecords(r1, r2 *protocol.Record) bool {
	if r1.Offset != r2.Offset {
		return false
	}

	if !r1.Time.Equal(r2.Time) {
		return false
	}

	k1 := ReadAll(r1.Key)
	k2 := ReadAll(r2.Key)

	if !reflect.DeepEqual(k1, k2) {
		return false
	}

	v1 := ReadAll(r1.Value)
	v2 := ReadAll(r2.Value)

	if !reflect.DeepEqual(v1, v2) {
		return false
	}

	return reflect.DeepEqual(r1.Headers, r2.Headers)
}

func ReadAll(bytes protocol.Bytes) []byte {
	b, err := protocol.ReadAll(bytes)
	if err != nil {
		panic(err)
	}
	return b
}
