package alterpartitionreassignments_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/alterpartitionreassignments"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0 = 0
)

func TestAlterPartitionReassignmentsRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &alterpartitionreassignments.Request{
		TimeoutMs: 1,
		Topics: []alterpartitionreassignments.RequestTopic{
			{
				Name: "topic-1",
				Partitions: []alterpartitionreassignments.RequestPartition{
					{
						PartitionIndex: 1,
						Replicas:       []int32{1, 2, 3},
					},
					{
						PartitionIndex: 2,
					},
				},
			},
		},
	})
}

func TestAlterPartitionReassignmentsResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &alterpartitionreassignments.Response{
		ErrorCode:      1,
		ErrorMessage:   "error",
		ThrottleTimeMs: 1,
		Results: []alterpartitionreassignments.ResponseResult{
			{
				Name: "topic-1",
				Partitions: []alterpartitionreassignments.ResponsePartition{
					{
						PartitionIndex: 1,
						ErrorMessage:   "error",
						ErrorCode:      1,
					},
					{
						PartitionIndex: 2,
					},
				},
			},
		},
	})
}
