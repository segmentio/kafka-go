package listpartitionreassignments_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/listpartitionreassignments"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0 = 0
)

func TestListPartitionReassignmentsRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &listpartitionreassignments.Request{
		Topics: []listpartitionreassignments.RequestTopic{
			{
				Name:             "topic-1",
				PartitionIndexes: []int32{1, 2, 3},
			},
		},
	})
}

func TestListPartitionReassignmentsResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &listpartitionreassignments.Response{
		Topics: []listpartitionreassignments.ResponseTopic{
			{
				Name: "topic-1",
				Partitions: []listpartitionreassignments.ResponsePartition{
					{
						PartitionIndex:   1,
						Replicas:         []int32{1, 2, 3},
						AddingReplicas:   []int32{4, 5, 6},
						RemovingReplicas: []int32{7, 8, 9},
					},
				},
			},
		},
	})
}
