package kafka

import (
	"bytes"
	"testing"
)

func TestGroupAssignment(t *testing.T) {
	testProtocolType(t,
		&groupAssignment{
			Version: 1,
			Topics: map[string][]int32{
				"a": {1, 2, 3},
				"b": {4, 5},
			},
			UserData: []byte(`blah`),
		},
		&groupAssignment{},
	)
}

func TestGroupAssignmentReadsFromZeroSize(t *testing.T) {
	rb := &readBuffer{
		r: bytes.NewReader(nil),
		n: 0,
	}

	item := groupAssignment{}
	item.readFrom(rb)

	if rb.err != nil {
		t.Fatal("unexpected error found in read buffer:", rb.err)
	}
	if rb.n != 0 {
		t.Fatalf("expected 0 remain, got %v", rb.err)
	}
	if item.Topics == nil {
		t.Error("expected non nil Topics to be assigned")
	}
}

func TestSyncGroupResponseV0(t *testing.T) {
	testProtocolType(t,
		&syncGroupResponseV0{
			ErrorCode:         2,
			MemberAssignments: []byte(`blah`),
		},
		&syncGroupResponseV0{},
	)
}
