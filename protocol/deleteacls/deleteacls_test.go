package deleteacls_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/deleteacls"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0 = 0
	v1 = 1
	v2 = 2
	v3 = 3
)

func TestDeleteACLsRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &deleteacls.Request{
		Filters: []deleteacls.RequestFilter{
			{
				ResourceTypeFilter: 2,
				ResourceNameFilter: "fake-topic-for-alice",
				PrincipalFilter:    "User:alice",
				HostFilter:         "*",
				Operation:          3,
				PermissionType:     3,
			},
		},
	})

	prototest.TestRequest(t, v1, &deleteacls.Request{
		Filters: []deleteacls.RequestFilter{
			{
				ResourceTypeFilter:        2,
				ResourceNameFilter:        "fake-topic-for-alice",
				ResourcePatternTypeFilter: 0,
				PrincipalFilter:           "User:alice",
				HostFilter:                "*",
				Operation:                 3,
				PermissionType:            3,
			},
		},
	})

	prototest.TestRequest(t, v2, &deleteacls.Request{
		Filters: []deleteacls.RequestFilter{
			{
				ResourceTypeFilter:        2,
				ResourceNameFilter:        "fake-topic-for-alice",
				ResourcePatternTypeFilter: 0,
				PrincipalFilter:           "User:alice",
				HostFilter:                "*",
				Operation:                 3,
				PermissionType:            3,
			},
		},
	})

	prototest.TestRequest(t, v3, &deleteacls.Request{
		Filters: []deleteacls.RequestFilter{
			{
				ResourceTypeFilter:        2,
				ResourceNameFilter:        "fake-topic-for-alice",
				ResourcePatternTypeFilter: 0,
				PrincipalFilter:           "User:alice",
				HostFilter:                "*",
				Operation:                 3,
				PermissionType:            3,
			},
		},
	})
}

func TestDeleteACLsResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &deleteacls.Response{
		ThrottleTimeMs: 1,
		FilterResults: []deleteacls.FilterResult{
			{
				ErrorCode:    1,
				ErrorMessage: "foo",
				MatchingACLs: []deleteacls.MatchingACL{
					{
						ErrorCode:      1,
						ErrorMessage:   "bar",
						ResourceType:   2,
						ResourceName:   "fake-topic-for-alice",
						Principal:      "User:alice",
						Host:           "*",
						Operation:      3,
						PermissionType: 3,
					},
				},
			},
		},
	})

	prototest.TestResponse(t, v1, &deleteacls.Response{
		ThrottleTimeMs: 1,
		FilterResults: []deleteacls.FilterResult{
			{
				ErrorCode:    1,
				ErrorMessage: "foo",
				MatchingACLs: []deleteacls.MatchingACL{
					{
						ErrorCode:           1,
						ErrorMessage:        "bar",
						ResourceType:        2,
						ResourceName:        "fake-topic-for-alice",
						ResourcePatternType: 0,
						Principal:           "User:alice",
						Host:                "*",
						Operation:           3,
						PermissionType:      3,
					},
				},
			},
		},
	})

	prototest.TestResponse(t, v2, &deleteacls.Response{
		ThrottleTimeMs: 1,
		FilterResults: []deleteacls.FilterResult{
			{
				ErrorCode:    1,
				ErrorMessage: "foo",
				MatchingACLs: []deleteacls.MatchingACL{
					{
						ErrorCode:           1,
						ErrorMessage:        "bar",
						ResourceType:        2,
						ResourceName:        "fake-topic-for-alice",
						ResourcePatternType: 0,
						Principal:           "User:alice",
						Host:                "*",
						Operation:           3,
						PermissionType:      3,
					},
				},
			},
		},
	})

	prototest.TestResponse(t, v3, &deleteacls.Response{
		ThrottleTimeMs: 1,
		FilterResults: []deleteacls.FilterResult{
			{
				ErrorCode:    1,
				ErrorMessage: "foo",
				MatchingACLs: []deleteacls.MatchingACL{
					{
						ErrorCode:           1,
						ErrorMessage:        "bar",
						ResourceType:        2,
						ResourceName:        "fake-topic-for-alice",
						ResourcePatternType: 0,
						Principal:           "User:alice",
						Host:                "*",
						Operation:           3,
						PermissionType:      3,
					},
				},
			},
		},
	})
}
