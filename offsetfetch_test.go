package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestOffsetFetchResponseV3(t *testing.T) {
	item := offsetFetchResponseV3{
		ThrottleTimeMS: 1,
		Responses: []offsetFetchResponseV3Response{
			{
				Topic: "a",
				PartitionResponses: []offsetFetchResponseV3PartitionResponse{
					{
						Partition: 2,
						Offset:    3,
						Metadata:  "b",
						ErrorCode: 4,
					},
				},
			},
		},
		ErrorCode: 5,
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found offsetFetchResponseV3
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
