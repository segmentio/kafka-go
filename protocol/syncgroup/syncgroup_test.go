package syncgroup_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/prototest"
	"github.com/segmentio/kafka-go/protocol/syncgroup"
)

func TestSyncGroupReq(t *testing.T) {
	for _, version := range []int16{0, 1, 2} {
		prototest.TestRequest(t, version, &syncgroup.Request{
			GroupID:      "group-id-1",
			GenerationID: 10,
			MemberID:     "member-id-1",
			Assignments: []syncgroup.RequestAssignment{
				{
					MemberID:   "member-id-2",
					Assignment: []byte{0, 1, 2, 3, 4},
				},
			},
		})
	}

	// Version 3 added:
	// GroupInstanceID
	for _, version := range []int16{3, 4} {
		prototest.TestRequest(t, version, &syncgroup.Request{
			GroupID:         "group-id-1",
			GenerationID:    10,
			MemberID:        "member-id-1",
			GroupInstanceID: "group-instance-id",
			Assignments: []syncgroup.RequestAssignment{
				{
					MemberID:   "member-id-2",
					Assignment: []byte{0, 1, 2, 3, 4},
				},
			},
		})
	}

	// Version 5 added
	// ProtocolType
	// ProtocolName
	for _, version := range []int16{5} {
		prototest.TestRequest(t, version, &syncgroup.Request{
			GroupID:         "group-id-1",
			GenerationID:    10,
			MemberID:        "member-id-1",
			GroupInstanceID: "group-instance-id",
			ProtocolType:    "protocol-type",
			ProtocolName:    "protocol-name",
			Assignments: []syncgroup.RequestAssignment{
				{
					MemberID:   "member-id-2",
					Assignment: []byte{0, 1, 2, 3, 4},
				},
			},
		})
	}
}

func TestSyncGroupResp(t *testing.T) {
	for _, version := range []int16{0} {
		prototest.TestResponse(t, version, &syncgroup.Response{
			ErrorCode:   10,
			Assignments: []byte{0, 1, 2, 3, 4},
		})
	}

	// Version 1 added
	// ThrottleTimeMS
	for _, version := range []int16{1, 2, 3, 4} {
		prototest.TestResponse(t, version, &syncgroup.Response{
			ErrorCode:      10,
			ThrottleTimeMS: 1,
			Assignments:    []byte{0, 1, 2, 3, 4},
		})
	}

	// Version 5 added
	// ProtocolType
	// ProtocolName
	for _, version := range []int16{5} {
		prototest.TestResponse(t, version, &syncgroup.Response{
			ErrorCode:      10,
			ThrottleTimeMS: 1,
			ProtocolType:   "protocol-type",
			ProtocolName:   "protocol-name",
			Assignments:    []byte{0, 1, 2, 3, 4},
		})
	}
}
