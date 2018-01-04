package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestFindCoordinatorResponseV1(t *testing.T) {
	item := findCoordinatorResponseV1{
		ThrottleTimeMS: 1,
		ErrorCode:      2,
		ErrorMessage:   "a",
		Coordinator: findCoordinatorResponseCoordinatorV1{
			NodeID: 3,
			Host:   "b",
			Port:   4,
		},
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found findCoordinatorResponseV1
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
