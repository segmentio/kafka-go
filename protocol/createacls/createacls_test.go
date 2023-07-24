package createacls_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/createacls"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0 = 0
	v1 = 1
	v2 = 2
	v3 = 3
)

func TestCreateACLsRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &createacls.Request{
		Creations: []createacls.RequestACLs{
			{
				Principal:      "User:alice",
				PermissionType: 3,
				Operation:      3,
				ResourceType:   2,
				ResourceName:   "fake-topic-for-alice",
				Host:           "*",
			},
		},
	})

	prototest.TestRequest(t, v1, &createacls.Request{
		Creations: []createacls.RequestACLs{
			{
				Principal:           "User:alice",
				PermissionType:      3,
				Operation:           3,
				ResourceType:        2,
				ResourcePatternType: 3,
				ResourceName:        "fake-topic-for-alice",
				Host:                "*",
			},
		},
	})

	prototest.TestRequest(t, v2, &createacls.Request{
		Creations: []createacls.RequestACLs{
			{
				Principal:           "User:alice",
				PermissionType:      3,
				Operation:           3,
				ResourceType:        2,
				ResourcePatternType: 3,
				ResourceName:        "fake-topic-for-alice",
				Host:                "*",
			},
		},
	})

	prototest.TestRequest(t, v3, &createacls.Request{
		Creations: []createacls.RequestACLs{
			{
				Principal:           "User:alice",
				PermissionType:      3,
				Operation:           3,
				ResourceType:        2,
				ResourcePatternType: 3,
				ResourceName:        "fake-topic-for-alice",
				Host:                "*",
			},
		},
	})
}

func TestCreateACLsResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &createacls.Response{
		ThrottleTimeMs: 1,
		Results: []createacls.ResponseACLs{
			{
				ErrorCode:    1,
				ErrorMessage: "foo",
			},
		},
	})

	prototest.TestResponse(t, v1, &createacls.Response{
		ThrottleTimeMs: 1,
		Results: []createacls.ResponseACLs{
			{
				ErrorCode:    1,
				ErrorMessage: "foo",
			},
		},
	})

	prototest.TestResponse(t, v2, &createacls.Response{
		ThrottleTimeMs: 1,
		Results: []createacls.ResponseACLs{
			{
				ErrorCode:    1,
				ErrorMessage: "foo",
			},
		},
	})

	prototest.TestResponse(t, v3, &createacls.Response{
		ThrottleTimeMs: 1,
		Results: []createacls.ResponseACLs{
			{
				ErrorCode:    1,
				ErrorMessage: "foo",
			},
		},
	})

}
