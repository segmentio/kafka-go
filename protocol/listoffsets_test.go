package protocol

import "testing"

func TestListOffsetsRequest(t *testing.T) {
	testRequest(t, v1, &ListOffsetsRequest{
		ReplicaID: 1,
		Topics: []ListOffsetsRequestTopic{
			{
				Topic: "topic-1",
				Partitions: []ListOffsetsRequestPartition{
					{Partition: 0, Timestamp: 1e9},
					{Partition: 1, Timestamp: 1e9},
					{Partition: 2, Timestamp: 1e9},
				},
			},
		},
	})

	testRequest(t, v4, &ListOffsetsRequest{
		ReplicaID:      1,
		IsolationLevel: 2,
		Topics: []ListOffsetsRequestTopic{
			{
				Topic: "topic-1",
				Partitions: []ListOffsetsRequestPartition{
					{Partition: 0, Timestamp: 1e9},
					{Partition: 1, Timestamp: 1e9},
					{Partition: 2, Timestamp: 1e9},
				},
			},
			{
				Topic: "topic-2",
				Partitions: []ListOffsetsRequestPartition{
					{Partition: 0, CurrentLeaderEpoch: 10, Timestamp: 1e9},
					{Partition: 1, CurrentLeaderEpoch: 11, Timestamp: 1e9},
					{Partition: 2, CurrentLeaderEpoch: 12, Timestamp: 1e9},
				},
			},
		},
	})
}

func TestListOffsetsResponse(t *testing.T) {
	testResponse(t, v1, &ListOffsetsResponse{
		Topics: []ListOffsetsResponseTopic{
			{
				Topic: "topic-1",
				Partitions: []ListOffsetsResponsePartition{
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

	testResponse(t, v4, &ListOffsetsResponse{
		Topics: []ListOffsetsResponseTopic{
			{
				Topic: "topic-1",
				Partitions: []ListOffsetsResponsePartition{
					{
						ThrottleTimeMs: 1234,
						Partition:      0,
						ErrorCode:      0,
						Timestamp:      1e9,
						Offset:         1234567890,
						LeaderEpoch:    10,
					},
				},
			},
			{
				Topic: "topic-2",
				Partitions: []ListOffsetsResponsePartition{
					{
						ThrottleTimeMs: 1234,
						Partition:      0,
						ErrorCode:      0,
						Timestamp:      1e9,
						Offset:         1234567890,
						LeaderEpoch:    10,
					},
					{
						ThrottleTimeMs: 1234,
						Partition:      1,
						ErrorCode:      2,
					},
				},
			},
		},
	})
}
