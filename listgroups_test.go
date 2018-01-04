package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestListGroupsResponseV1(t *testing.T) {
	item := listGroupsResponseV1{
		ErrorCode: 2,
		Groups: []ListGroupsResponseGroupV1{
			{
				GroupID:      "a",
				ProtocolType: "b",
			},
		},
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found listGroupsResponseV1
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
