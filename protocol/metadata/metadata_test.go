package metadata_test

import (
	"testing"

	"github.com/apoorvag-mav/kafka-go/protocol/metadata"
	"github.com/apoorvag-mav/kafka-go/protocol/prototest"
)

const (
	v0 = 0
	v1 = 1
	v4 = 4
	v8 = 8
)

func TestMetadataRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &metadata.Request{
		TopicNames: nil,
	})

	prototest.TestRequest(t, v4, &metadata.Request{
		TopicNames:             []string{"hello", "world"},
		AllowAutoTopicCreation: true,
	})

	prototest.TestRequest(t, v8, &metadata.Request{
		TopicNames:                         []string{"hello", "world"},
		AllowAutoTopicCreation:             true,
		IncludeClusterAuthorizedOperations: true,
		IncludeTopicAuthorizedOperations:   true,
	})
}

func TestMetadataResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &metadata.Response{
		Brokers: []metadata.ResponseBroker{
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
		Topics: []metadata.ResponseTopic{
			{
				Name: "topic-1",
				Partitions: []metadata.ResponsePartition{
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

	prototest.TestResponse(t, v1, &metadata.Response{
		ControllerID: 1,
		Brokers: []metadata.ResponseBroker{
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
		Topics: []metadata.ResponseTopic{
			{
				Name:       "topic-1",
				IsInternal: true,
				Partitions: []metadata.ResponsePartition{
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

	prototest.TestResponse(t, v8, &metadata.Response{
		ThrottleTimeMs:              123,
		ClusterID:                   "test",
		ControllerID:                1,
		ClusterAuthorizedOperations: 0x01,
		Brokers: []metadata.ResponseBroker{
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
		Topics: []metadata.ResponseTopic{
			{
				Name: "topic-1",
				Partitions: []metadata.ResponsePartition{
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
	prototest.BenchmarkRequest(b, v8, &metadata.Request{
		TopicNames:                         []string{"hello", "world"},
		AllowAutoTopicCreation:             true,
		IncludeClusterAuthorizedOperations: true,
		IncludeTopicAuthorizedOperations:   true,
	})
}

func BenchmarkMetadataResponse(b *testing.B) {
	prototest.BenchmarkResponse(b, v8, &metadata.Response{
		ThrottleTimeMs:              123,
		ClusterID:                   "test",
		ControllerID:                1,
		ClusterAuthorizedOperations: 0x01,
		Brokers: []metadata.ResponseBroker{
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
		Topics: []metadata.ResponseTopic{
			{
				Name: "topic-1",
				Partitions: []metadata.ResponsePartition{
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
