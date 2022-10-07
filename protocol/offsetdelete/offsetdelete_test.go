package offsetdelete_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/offsetdelete"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

func TestOffsetDeleteequest(t *testing.T) {
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
