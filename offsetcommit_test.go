package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestOffsetCommitResponseV2(t *testing.T) {
	item := offsetCommitResponseV2{
		Responses: []offsetCommitResponseV2Response{
			{
				Topic: "a",
				PartitionResponses: []offsetCommitResponseV2PartitionResponse{
					{
						Partition: 1,
						ErrorCode: 2,
					},
				},
			},
		},
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found offsetCommitResponseV2
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
