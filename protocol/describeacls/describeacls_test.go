package describeacls_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/describeacls"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0 = 0
	v1 = 1
	v2 = 2
	v3 = 3
)

func TestDescribeACLsRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &describeacls.Request{
		Filter: describeacls.ACLFilter{
			ResourceTypeFilter: 2,
			ResourceNameFilter: "fake-topic-for-alice",
			PrincipalFilter:    "User:alice",
			HostFilter:         "*",
			Operation:          3,
			PermissionType:     3,
		},
	})

	prototest.TestRequest(t, v1, &describeacls.Request{
		Filter: describeacls.ACLFilter{
			ResourceTypeFilter:        2,
			ResourceNameFilter:        "fake-topic-for-alice",
			ResourcePatternTypeFilter: 0,
			PrincipalFilter:           "User:alice",
			HostFilter:                "*",
			Operation:                 3,
			PermissionType:            3,
		},
	})

	prototest.TestRequest(t, v2, &describeacls.Request{
		Filter: describeacls.ACLFilter{
			ResourceTypeFilter:        2,
			ResourceNameFilter:        "fake-topic-for-alice",
			ResourcePatternTypeFilter: 0,
			PrincipalFilter:           "User:alice",
			HostFilter:                "*",
			Operation:                 3,
			PermissionType:            3,
		},
	})

	prototest.TestRequest(t, v3, &describeacls.Request{
		Filter: describeacls.ACLFilter{
			ResourceTypeFilter:        2,
			ResourceNameFilter:        "fake-topic-for-alice",
			ResourcePatternTypeFilter: 0,
			PrincipalFilter:           "User:alice",
			HostFilter:                "*",
			Operation:                 3,
			PermissionType:            3,
		},
	})
}

func TestDescribeACLsResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &describeacls.Response{
		ThrottleTimeMs: 1,
		ErrorCode:      1,
		ErrorMessage:   "foo",
		Resources: []describeacls.Resource{
			{
				ResourceType: 2,
				ResourceName: "fake-topic-for-alice",
				ACLs: []describeacls.ResponseACL{
					{
						Principal:      "User:alice",
						Host:           "*",
						Operation:      3,
						PermissionType: 3,
					},
				},
			},
		},
	})

	prototest.TestResponse(t, v1, &describeacls.Response{
		ThrottleTimeMs: 1,
		ErrorCode:      1,
		ErrorMessage:   "foo",
		Resources: []describeacls.Resource{
			{
				ResourceType: 2,
				ResourceName: "fake-topic-for-alice",
				PatternType:  3,
				ACLs: []describeacls.ResponseACL{
					{
						Principal:      "User:alice",
						Host:           "*",
						Operation:      3,
						PermissionType: 3,
					},
				},
			},
		},
	})

	prototest.TestResponse(t, v2, &describeacls.Response{
		ThrottleTimeMs: 1,
		ErrorCode:      1,
		ErrorMessage:   "foo",
		Resources: []describeacls.Resource{
			{
				ResourceType: 2,
				ResourceName: "fake-topic-for-alice",
				PatternType:  3,
				ACLs: []describeacls.ResponseACL{
					{
						Principal:      "User:alice",
						Host:           "*",
						Operation:      3,
						PermissionType: 3,
					},
				},
			},
		},
	})

	prototest.TestResponse(t, v3, &describeacls.Response{
		ThrottleTimeMs: 1,
		ErrorCode:      1,
		ErrorMessage:   "foo",
		Resources: []describeacls.Resource{
			{
				ResourceType: 2,
				ResourceName: "fake-topic-for-alice",
				PatternType:  3,
				ACLs: []describeacls.ResponseACL{
					{
						Principal:      "User:alice",
						Host:           "*",
						Operation:      3,
						PermissionType: 3,
					},
				},
			},
		},
	})
}
