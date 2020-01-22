package protocol

import "testing"

func TestMetadataRequest(t *testing.T) {
	testRequest(t, v0, &MetadataRequest{
		TopicNames: nil,
	})

	testRequest(t, v4, &MetadataRequest{
		TopicNames:             []string{"hello", "world"},
		AllowAutoTopicCreation: true,
	})

	testRequest(t, v8, &MetadataRequest{
		TopicNames:                          []string{"hello", "world"},
		AllowAutoTopicCreation:              true,
		IncludeCustomerAuthorizedOperations: true,
		IncludeTopicAuthorizedOperations:    true,
	})
}

func TestMetadataResponse(t *testing.T) {
	testResponse(t, v0, &MetadataResponse{
		Brokers: []MetadataResponseBroker{
			{
				NodeID: 0,
				Host:   "127.0.0.1",
				Port:   9092,
			},
			{
				NodeID: 1,
				Host:   "127.0.0.1",
				Port:   9093,
			},
		},
		Topics: []MetadataResponseTopic{
			{
				Name: "topic-1",
				Partitions: []MetadataResponsePartition{
					{
						PartitionIndex: 0,
						LeaderID:       1,
						ReplicaNodes:   []int32{0},
						IsrNodes:       []int32{1, 0},
					},
				},
			},
		},
	})

	testResponse(t, v1, &MetadataResponse{
		ControllerID: 1,
		Brokers: []MetadataResponseBroker{
			{
				NodeID: 0,
				Host:   "127.0.0.1",
				Port:   9092,
				Rack:   "rack-1",
			},
			{
				NodeID: 1,
				Host:   "127.0.0.1",
				Port:   9093,
				Rack:   "rack-2",
			},
		},
		Topics: []MetadataResponseTopic{
			{
				Name:       "topic-1",
				IsInternal: true,
				Partitions: []MetadataResponsePartition{
					{
						PartitionIndex: 0,
						LeaderID:       1,
						ReplicaNodes:   []int32{0},
						IsrNodes:       []int32{1, 0},
					},
					{
						PartitionIndex: 1,
						LeaderID:       0,
						ReplicaNodes:   []int32{1},
						IsrNodes:       []int32{0, 1},
					},
				},
			},
		},
	})

	testResponse(t, v9, &MetadataResponse{
		ThrottleTimeMs:              123,
		ControllerID:                1,
		ClientID:                    "me",
		ClusterAuthorizedOperations: 0x01,
		Brokers: []MetadataResponseBroker{
			{
				NodeID: 0,
				Host:   "127.0.0.1",
				Port:   9092,
				Rack:   "rack-1",
			},
			{
				NodeID: 1,
				Host:   "127.0.0.1",
				Port:   9093,
				Rack:   "rack-2",
			},
		},
		Topics: []MetadataResponseTopic{
			{
				Name: "topic-1",
				Partitions: []MetadataResponsePartition{
					{
						PartitionIndex:  0,
						LeaderID:        1,
						LeaderEpoch:     1234567890,
						ReplicaNodes:    []int32{0},
						IsrNodes:        []int32{0},
						OfflineReplicas: []int32{1},
					},
					{
						ErrorCode:       1,
						ReplicaNodes:    []int32{},
						IsrNodes:        []int32{},
						OfflineReplicas: []int32{},
					},
				},
				TopicAuthorizedOperations: 0x01,
			},
		},
	})
}

func BenchmarkMetadataRequest(b *testing.B) {
	benchmarkRequest(b, v8, &MetadataRequest{
		TopicNames:                          []string{"hello", "world"},
		AllowAutoTopicCreation:              true,
		IncludeCustomerAuthorizedOperations: true,
		IncludeTopicAuthorizedOperations:    true,
	})
}

func BenchmarkMetadataResponse(b *testing.B) {
	benchmarkResponse(b, v9, &MetadataResponse{
		ThrottleTimeMs:              123,
		ControllerID:                1,
		ClientID:                    "me",
		ClusterAuthorizedOperations: 0x01,
		Brokers: []MetadataResponseBroker{
			{
				NodeID: 0,
				Host:   "127.0.0.1",
				Port:   9092,
				Rack:   "rack-1",
			},
			{
				NodeID: 1,
				Host:   "127.0.0.1",
				Port:   9093,
				Rack:   "rack-2",
			},
		},
		Topics: []MetadataResponseTopic{
			{
				Name: "topic-1",
				Partitions: []MetadataResponsePartition{
					{
						PartitionIndex:  0,
						LeaderID:        1,
						LeaderEpoch:     1234567890,
						ReplicaNodes:    []int32{0},
						IsrNodes:        []int32{0},
						OfflineReplicas: []int32{1},
					},
					{
						ErrorCode:       1,
						ReplicaNodes:    []int32{},
						IsrNodes:        []int32{},
						OfflineReplicas: []int32{},
					},
				},
				TopicAuthorizedOperations: 0x01,
			},
		},
	})

}
