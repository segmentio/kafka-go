package offsetdelete_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/offsetdelete"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

func TestOffsetDeleteRequest(t *testing.T) {
	for _, version := range []int16{0} {
		prototest.TestRequest(t, version, &offsetdelete.Request{
			GroupID: "group-0",
			Topics: []offsetdelete.RequestTopic{
				{
					Name: "topic-0",
					Partitions: []offsetdelete.RequestPartition{
						{
							PartitionIndex: 0,
						},
						{
							PartitionIndex: 1,
						},
					},
				},
			},
		})
	}
}

func TestOffsetDeleteResponse(t *testing.T) {
	for _, version := range []int16{0} {
		prototest.TestResponse(t, version, &offsetdelete.Response{
			ErrorCode: 0,
			Topics: []offsetdelete.ResponseTopic{
				{
					Name: "topic-0",
					Partitions: []offsetdelete.ResponsePartition{
						{
							PartitionIndex: 0,
							ErrorCode:      1,
						},
						{
							PartitionIndex: 1,
							ErrorCode:      1,
						},
					},
				},
			},
		})
	}
}
