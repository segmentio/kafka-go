package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestOffsetFetchResponseV1(t *testing.T) {
	item := offsetFetchResponseV1{
		Responses: []offsetFetchResponseV1Response{
			{
				Topic: "a",
				PartitionResponses: []offsetFetchResponseV1PartitionResponse{
					{
						Partition: 2,
						Offset:    3,
						Metadata:  "b",
						ErrorCode: 4,
					},
				},
			},
		},
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found offsetFetchResponseV1
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
