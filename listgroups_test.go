package kafka

import (
	"testing"
)

func TestListGroupsResponseV1(t *testing.T) {
	testProtocolType(t,
		&listGroupsResponseV1{
			ErrorCode: 2,
			Groups: []ListGroupsResponseGroupV1{{
				GroupID:      "a",
				ProtocolType: "b",
			}},
		},
		&listGroupsResponseV1{},
	)
}
