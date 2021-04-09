package listoffsets_test

import (
	"testing"

	"github.com/apoorvag-mav/kafka-go/protocol/listoffsets"
	"github.com/apoorvag-mav/kafka-go/protocol/prototest"
)

const (
	v1 = 1
	v4 = 4
)

func TestListOffsetsRequest(t *testing.T) {
	prototest.TestRequest(t, v1, &listoffsets.Request{
		ReplicaID: 1,
		Topics: []listoffsets.RequestTopic{
			{
				Topic: "topic-1",
				Partitions: []listoffsets.RequestPartition{
					{Partition: 0, Timestamp: 1e9},
					{Partition: 1, Timestamp: 1e9},
					{Partition: 2, Timestamp: 1e9},
				},
			},
		},
	})

	prototest.TestRequest(t, v4, &listoffsets.Request{
		ReplicaID:      1,
		IsolationLevel: 2,
		Topics: []listoffsets.RequestTopic{
			{
				Topic: "topic-1",
				Partitions: []listoffsets.RequestPartition{
					{Partition: 0, Timestamp: 1e9},
					{Partition: 1, Timestamp: 1e9},
					{Partition: 2, Timestamp: 1e9},
				},
			},
			{
				Topic: "topic-2",
				Partitions: []listoffsets.RequestPartition{
					{Partition: 0, CurrentLeaderEpoch: 10, Timestamp: 1e9},
					{Partition: 1, CurrentLeaderEpoch: 11, Timestamp: 1e9},
					{Partition: 2, CurrentLeaderEpoch: 12, Timestamp: 1e9},
				},
			},
		},
	})
}

func TestListOffsetsResponse(t *testing.T) {
	prototest.TestResponse(t, v1, &listoffsets.Response{
		Topics: []listoffsets.ResponseTopic{
			{
				Topic: "topic-1",
				Partitions: []listoffsets.ResponsePartition{
					{
						Partition: 0,
						ErrorCode: 0,
						Timestamp: 1e9,
						Offset:    1234567890,
					},
				},
			},
		},
	})

	prototest.TestResponse(t, v4, &listoffsets.Response{
		ThrottleTimeMs: 1234,
		Topics: []listoffsets.ResponseTopic{
			{
				Topic: "topic-1",
				Partitions: []listoffsets.ResponsePartition{
					{
						Partition:   0,
						ErrorCode:   0,
						Timestamp:   1e9,
						Offset:      1234567890,
						LeaderEpoch: 10,
					},
				},
			},
			{
				Topic: "topic-2",
				Partitions: []listoffsets.ResponsePartition{
					{
						Partition:   0,
						ErrorCode:   0,
						Timestamp:   1e9,
						Offset:      1234567890,
						LeaderEpoch: 10,
					},
					{
						Partition: 1,
						ErrorCode: 2,
					},
				},
			},
		},
	})
}
