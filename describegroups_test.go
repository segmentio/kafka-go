package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestDescribeGroupsResponseV0(t *testing.T) {
	item := describeGroupsResponseV0{
		Groups: []describeGroupsResponseGroupV0{
			{
				ErrorCode:    2,
				GroupID:      "a",
				State:        "b",
				ProtocolType: "c",
				Protocol:     "d",
				Members: []describeGroupsResponseMemberV0{
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

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found describeGroupsResponseV0
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
