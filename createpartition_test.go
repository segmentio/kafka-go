package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestCreatePartitionsResponseV0(t *testing.T) {
	item := createPartitionsResponseV0{
		ThrottleTimeMs: 100,
		Results: []createPartitionsResponseV0Result{
			{
				Name:      "test",
				ErrorCode: 0,
			},
		},
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found createPartitionsResponseV0
	remain, err := (&found).readFrom(bufio.NewReader(b), b.Len())
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
