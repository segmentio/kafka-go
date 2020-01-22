package protocol

import "testing"

func TestFetchRequest(t *testing.T) {
	testRequest(t, v0, &FetchRequest{
		ReplicaID:   -1,
		MaxWaitTime: 500,
		MinBytes:    1024,
		Topics: []FetchRequestTopic{
			{
				Topic: "topic-1",
				Partitions: []FetchRequestPartition{
					{
						Partition:         1,
						FetchOffset:       2,
						PartitionMaxBytes: 1024,
					},
				},
			},
		},
	})
}

func TestFetchResponse(t *testing.T) {
	testResponse(t, v0, &FetchResponse{
		Topics: []FetchResponseTopic{
			{
				Topic: "topic-1",
				Partitions: []FetchResponsePartition{
					{
						Partition:     1,
						HighWatermark: 1000,
						RecordSet: RecordSet{
							Version: 1,
							Records: []Record{},
						},
					},
				},
			},
		},
	})
}
