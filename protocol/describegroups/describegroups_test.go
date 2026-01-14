package describegroups_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/describegroups"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0 = 0
	v1 = 1
	v3 = 3
	v4 = 4
	v5 = 5
)

func TestDescribeGroupsRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &describegroups.Request{
		Groups: []string{"test-group"},
	})

	prototest.TestRequest(t, v1, &describegroups.Request{
		Groups: []string{"test-group"},
	})

	prototest.TestRequest(t, v3, &describegroups.Request{
		Groups:                      []string{"test-group"},
		IncludeAuthorizedOperations: true,
	})

	prototest.TestRequest(t, v4, &describegroups.Request{
		Groups:                      []string{"test-group"},
		IncludeAuthorizedOperations: true,
	})

	prototest.TestRequest(t, v5, &describegroups.Request{
		Groups:                      []string{"test-group"},
		IncludeAuthorizedOperations: true,
	})
}

func TestDescribeGroupsResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &describegroups.Response{
		Groups: []describegroups.ResponseGroup{
			{
				ErrorCode:    0,
				GroupID:      "test-group",
				GroupState:   "Stable",
				ProtocolType: "consumer",
				ProtocolData: "range",
				Members: []describegroups.ResponseGroupMember{
					{
						MemberID:         "consumer-1",
						ClientID:         "client-1",
						ClientHost:       "/127.0.0.1",
						MemberMetadata:   []byte{0x00, 0x01},
						MemberAssignment: []byte{0x00, 0x02},
					},
				},
			},
		},
	})

	prototest.TestResponse(t, v1, &describegroups.Response{
		ThrottleTimeMs: 100,
		Groups: []describegroups.ResponseGroup{
			{
				ErrorCode:    0,
				GroupID:      "test-group",
				GroupState:   "Stable",
				ProtocolType: "consumer",
				ProtocolData: "range",
				Members: []describegroups.ResponseGroupMember{
					{
						MemberID:         "consumer-1",
						ClientID:         "client-1",
						ClientHost:       "/127.0.0.1",
						MemberMetadata:   []byte{0x00, 0x01},
						MemberAssignment: []byte{0x00, 0x02},
					},
				},
			},
		},
	})

	prototest.TestResponse(t, v3, &describegroups.Response{
		ThrottleTimeMs: 100,
		Groups: []describegroups.ResponseGroup{
			{
				ErrorCode:            0,
				GroupID:              "test-group",
				GroupState:           "Stable",
				ProtocolType:         "consumer",
				ProtocolData:         "range",
				AuthorizedOperations: 2147483647,
				Members: []describegroups.ResponseGroupMember{
					{
						MemberID:         "consumer-1",
						ClientID:         "client-1",
						ClientHost:       "/127.0.0.1",
						MemberMetadata:   []byte{0x00, 0x01},
						MemberAssignment: []byte{0x00, 0x02},
					},
				},
			},
		},
	})

	prototest.TestResponse(t, v4, &describegroups.Response{
		ThrottleTimeMs: 100,
		Groups: []describegroups.ResponseGroup{
			{
				ErrorCode:            0,
				GroupID:              "test-group",
				GroupState:           "Stable",
				ProtocolType:         "consumer",
				ProtocolData:         "range",
				AuthorizedOperations: 2147483647,
				Members: []describegroups.ResponseGroupMember{
					{
						MemberID:         "consumer-1",
						GroupInstanceID:  "instance-1",
						ClientID:         "client-1",
						ClientHost:       "/127.0.0.1",
						MemberMetadata:   []byte{0x00, 0x01},
						MemberAssignment: []byte{0x00, 0x02},
					},
				},
			},
		},
	})

	prototest.TestResponse(t, v5, &describegroups.Response{
		ThrottleTimeMs: 100,
		Groups: []describegroups.ResponseGroup{
			{
				ErrorCode:            0,
				GroupID:              "test-group",
				GroupState:           "Stable",
				ProtocolType:         "consumer",
				ProtocolData:         "range",
				AuthorizedOperations: 2147483647,
				Members: []describegroups.ResponseGroupMember{
					{
						MemberID:         "consumer-1",
						GroupInstanceID:  "instance-1",
						ClientID:         "client-1",
						ClientHost:       "/127.0.0.1",
						MemberMetadata:   []byte{0x00, 0x01},
						MemberAssignment: []byte{0x00, 0x02},
					},
				},
			},
		},
	})
}
