package kafka

import (
	"bufio"
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestGroupAssignment(t *testing.T) {
	item := groupAssignment{
		Version: 1,
		Topics: map[string][]int32{
			"a": {1, 2, 3},
			"b": {4, 5},
		},
		UserData: []byte(`blah`),
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found groupAssignment
	remain, err := (&found).readFrom(bufio.NewReader(buf), buf.Len())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if remain != 0 {
		t.Errorf("expected 0 remain, got %v", remain)
		t.FailNow()
	}
	if !reflect.DeepEqual(item, found) {
		t.Error("expected item and found to be the same")
		t.FailNow()
	}
}

func TestGroupAssignmentReadsFromZeroSize(t *testing.T) {
	var item groupAssignment
	remain, err := (&item).readFrom(bufio.NewReader(bytes.NewReader(nil)), 0)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if remain != 0 {
		t.Errorf("expected 0 remain, got %v", remain)
		t.FailNow()
	}
	if item.Topics == nil {
		t.Error("expected non nil Topics to be assigned")
	}
}

func TestSyncGroupResponseV1(t *testing.T) {
	item := syncGroupResponseV1{
		ThrottleTimeMS:    1,
		ErrorCode:         2,
		MemberAssignments: []byte(`blah`),
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found syncGroupResponseV1
	remain, err := (&found).readFrom(bufio.NewReader(buf), buf.Len())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if remain != 0 {
		t.Errorf("expected 0 remain, got %v", remain)
		t.FailNow()
	}
	if !reflect.DeepEqual(item, found) {
		t.Error("expected item and found to be the same")
		t.FailNow()
	}
}

func BenchmarkSyncGroupResponseV1(t *testing.B) {
	item := syncGroupResponseV1{
		ThrottleTimeMS:    1,
		ErrorCode:         2,
		MemberAssignments: []byte(`blah`),
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	r := bytes.NewReader(buf.Bytes())
	reader := bufio.NewReader(r)
	size := buf.Len()

	for i := 0; i < t.N; i++ {
		r.Seek(0, io.SeekStart)
		var found syncGroupResponseV1
		remain, err := (&found).readFrom(reader, size)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
		if remain != 0 {
			t.Errorf("expected 0 remain, got %v", remain)
			t.FailNow()
		}
	}
}
