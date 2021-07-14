package createpartitions_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/createpartitions"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0 = 0
	v1 = 1
)

func TestCreatePartitionsRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &createpartitions.Request{
		Topics: []createpartitions.RequestTopic{
			{
				Name:  "foo",
				Count: 1,
				Assignments: []createpartitions.RequestAssignment{
					{
						BrokerIDs: []int32{1, 2, 3},
					},
				},
			},
		},
		TimeoutMs:    500,
		ValidateOnly: false,
	})

	prototest.TestRequest(t, v1, &createpartitions.Request{
		Topics: []createpartitions.RequestTopic{
			{
				Name:  "foo",
				Count: 1,
				Assignments: []createpartitions.RequestAssignment{
					{
						BrokerIDs: []int32{1, 2, 3},
					},
				},
			},
		},
		TimeoutMs:    500,
		ValidateOnly: false,
	})
}

func TestCreatePartitionsResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &createpartitions.Response{
		ThrottleTimeMs: 500,
		Results: []createpartitions.ResponseResult{
			{
				Name:         "foo",
				ErrorCode:    1,
				ErrorMessage: "foo",
			},
		},
	})

	prototest.TestResponse(t, v1, &createpartitions.Response{
		ThrottleTimeMs: 500,
		Results: []createpartitions.ResponseResult{
			{
				Name:         "foo",
				ErrorCode:    1,
				ErrorMessage: "foo",
			},
		},
	})
}
