package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestCreateTopicsResponseV0(t *testing.T) {
	item := createTopicsResponseV0{
		TopicErrors: []createTopicsResponseV0TopicError{
			{
				Topic:     "topic",
				ErrorCode: 2,
			},
		},
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found createTopicsResponseV0
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
