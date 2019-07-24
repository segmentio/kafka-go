package kafka

import (
	"testing"
)

func TestDescribeGroupsResponseV0(t *testing.T) {
	testProtocolType(t,
		&describeGroupsResponseV0{
			Groups: []describeGroupsResponseGroupV0{{
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
			}},
		},
		&describeGroupsResponseV0{},
	)
}
