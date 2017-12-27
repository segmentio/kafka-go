package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestDescribeGroupsResponseV1(t *testing.T) {
	item := describeGroupsResponseV1{
		ThrottleTimeMS: 1,
		Groups: []describeGroupsResponseGroupV1{
			{
				ErrorCode:    2,
				GroupID:      "a",
				State:        "b",
				ProtocolType: "c",
				Protocol:     "d",
				Members: []describeGroupsResponseMemberV1{
					{
						MemberID:          "e",
						ClientID:          "f",
						ClientHost:        "g",
						MemberMetadata:    []byte("h"),
						MemberAssignments: []byte("i"),
					},
				},
			},
		},
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found describeGroupsResponseV1
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
