package electleaders_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/electleaders"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0 = 0
	v1 = 1
)

func TestElectLeadersRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &electleaders.Request{
		TimeoutMs: 500,
		TopicPartitions: []electleaders.RequestTopicPartitions{
			{
				Topic:        "foo",
				PartitionIDs: []int32{100, 101, 102},
			},
		},
	})

	prototest.TestRequest(t, v1, &electleaders.Request{
		ElectionType: 1,
		TimeoutMs:    500,
		TopicPartitions: []electleaders.RequestTopicPartitions{
			{
				Topic:        "foo",
				PartitionIDs: []int32{100, 101, 102},
			},
		},
	})
}

func TestElectLeadersResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &electleaders.Response{
		ThrottleTime: 500,
		ReplicaElectionResults: []electleaders.ResponseReplicaElectionResult{
			{
				Topic: "foo",
				PartitionResults: []electleaders.ResponsePartitionResult{
					{PartitionID: 100, ErrorCode: 0, ErrorMessage: ""},
					{PartitionID: 101, ErrorCode: 0, ErrorMessage: ""},
					{PartitionID: 102, ErrorCode: 0, ErrorMessage: ""},
				},
			},
		},
	})

	prototest.TestResponse(t, v1, &electleaders.Response{
		ThrottleTime: 500,
		ErrorCode:    1,
		ReplicaElectionResults: []electleaders.ResponseReplicaElectionResult{
			{
				Topic: "foo",
				PartitionResults: []electleaders.ResponsePartitionResult{
					{PartitionID: 100, ErrorCode: 0, ErrorMessage: ""},
					{PartitionID: 101, ErrorCode: 0, ErrorMessage: ""},
					{PartitionID: 102, ErrorCode: 0, ErrorMessage: ""},
				},
			},
		},
	})
}
