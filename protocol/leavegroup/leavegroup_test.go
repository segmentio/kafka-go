package leavegroup_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/leavegroup"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

func TestLeaveGroupReq(t *testing.T) {
	for _, version := range []int16{0, 1, 2} {
		prototest.TestRequest(t, version, &leavegroup.Request{
			GroupID:  "group-id",
			MemberID: "member-id",
		})
	}

	// Version 3 added
	// Members
	// removed
	// MemberID
	for _, version := range []int16{3, 4} {
		prototest.TestRequest(t, version, &leavegroup.Request{
			GroupID: "group-id",
			Members: []leavegroup.RequestMember{
				{
					MemberID:        "member-id-1",
					GroupInstanceID: "group-instance-id",
				},
			},
		})
	}
}

func TestLeaveGroupResp(t *testing.T) {
	for _, version := range []int16{0} {
		prototest.TestResponse(t, version, &leavegroup.Response{
			ErrorCode: 10,
		})
	}

	// Version 1 added
	// ThrottleTimeMS
	for _, version := range []int16{1, 2} {
		prototest.TestResponse(t, version, &leavegroup.Response{
			ErrorCode:      10,
			ThrottleTimeMS: 100,
		})
	}

	// Version 3 added
	// Members
	for _, version := range []int16{3, 4} {
		prototest.TestResponse(t, version, &leavegroup.Response{
			ErrorCode:      10,
			ThrottleTimeMS: 100,
			Members: []leavegroup.ResponseMember{
				{
					MemberID:        "member-id-1",
					GroupInstanceID: "group-instance-id",
					ErrorCode:       10,
				},
			},
		})
	}
}
