package joingroup_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/joingroup"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

func TestJoinGroupReq(t *testing.T) {
	for _, version := range []int16{0} {
		prototest.TestRequest(t, version, &joingroup.Request{
			GroupID:          "group-id",
			SessionTimeoutMS: 10,
			MemberID:         "member-id",
			ProtocolType:     "protocol-type",
			Protocols: []joingroup.RequestProtocol{
				{
					Name:     "protocol-1",
					Metadata: []byte{0, 1, 2, 3, 4},
				},
			},
		})
	}

	// Version 1 added
	// RebalanceTimeoutMS
	for _, version := range []int16{1, 2, 3, 4} {
		prototest.TestRequest(t, version, &joingroup.Request{
			GroupID:            "group-id",
			SessionTimeoutMS:   10,
			RebalanceTimeoutMS: 10,
			MemberID:           "member-id",
			ProtocolType:       "protocol-type",
			Protocols: []joingroup.RequestProtocol{
				{
					Name:     "protocol-1",
					Metadata: []byte{0, 1, 2, 3, 4},
				},
			},
		})
	}

	// Version 5 added
	// GroupInstanceID
	for _, version := range []int16{5, 6, 7} {
		prototest.TestRequest(t, version, &joingroup.Request{
			GroupID:            "group-id",
			SessionTimeoutMS:   10,
			RebalanceTimeoutMS: 10,
			MemberID:           "member-id",
			ProtocolType:       "protocol-type",
			GroupInstanceID:    "group-instance-id",
			Protocols: []joingroup.RequestProtocol{
				{
					Name:     "protocol-1",
					Metadata: []byte{0, 1, 2, 3, 4},
				},
			},
		})
	}
}

func TestJoinGroupResp(t *testing.T) {
	for _, version := range []int16{0, 1} {
		prototest.TestResponse(t, version, &joingroup.Response{
			ErrorCode:    10,
			GenerationID: 10,
			ProtocolName: "protocol-name",
			LeaderID:     "leader",
			MemberID:     "member-id-1",
			Members: []joingroup.ResponseMember{
				{
					MemberID: "member-id-2",
					Metadata: []byte{0, 1, 2, 3, 4},
				},
			},
		})
	}

	// Version 2 added
	// ThrottleTimeMS
	for _, version := range []int16{2, 3, 4} {
		prototest.TestResponse(t, version, &joingroup.Response{
			ErrorCode:      10,
			GenerationID:   10,
			ThrottleTimeMS: 100,
			ProtocolName:   "protocol-name",
			LeaderID:       "leader",
			MemberID:       "member-id-1",
			Members: []joingroup.ResponseMember{
				{
					MemberID: "member-id-2",
					Metadata: []byte{0, 1, 2, 3, 4},
				},
			},
		})
	}

	// Version 5 added
	// ResponseMember.GroupInstanceID
	for _, version := range []int16{5, 6} {
		prototest.TestResponse(t, version, &joingroup.Response{
			ErrorCode:      10,
			GenerationID:   10,
			ThrottleTimeMS: 100,
			ProtocolName:   "protocol-name",
			LeaderID:       "leader",
			MemberID:       "member-id-1",
			Members: []joingroup.ResponseMember{
				{
					MemberID:        "member-id-2",
					Metadata:        []byte{0, 1, 2, 3, 4},
					GroupInstanceID: "group-instance-id",
				},
			},
		})
	}

	// Version 7 added
	// ProtocolType
	for _, version := range []int16{7} {
		prototest.TestResponse(t, version, &joingroup.Response{
			ErrorCode:      10,
			GenerationID:   10,
			ThrottleTimeMS: 100,
			ProtocolName:   "protocol-name",
			ProtocolType:   "protocol-type",
			LeaderID:       "leader",
			MemberID:       "member-id-1",
			Members: []joingroup.ResponseMember{
				{
					MemberID:        "member-id-2",
					Metadata:        []byte{0, 1, 2, 3, 4},
					GroupInstanceID: "group-instance-id",
				},
			},
		})
	}
}
