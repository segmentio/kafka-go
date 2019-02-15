package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestDeleteTopicsResponseV1(t *testing.T) {
	item := deleteTopicsResponseV0{
		TopicErrorCodes: []deleteTopicsResponseV0TopicErrorCode{
			{
				Topic:     "a",
				ErrorCode: 7,
			},
		},
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found deleteTopicsResponseV0
	remain, err := (&found).readFrom(bufio.NewReader(buf), buf.Len())
	if err != nil {
		t.Fatal(err)
	}
	if remain != 0 {
		t.Fatalf("expected 0 remain, got %v", remain)
	}
	if !reflect.DeepEqual(item, found) {
		t.Fatal("expected item and found to be the same")
	}
}
