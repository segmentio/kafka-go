package protocol

import (
	"testing"
	"time"
)

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
	t0 := time.Now().Truncate(time.Millisecond)
	t1 := t0.Add(1 * time.Millisecond)
	t2 := t0.Add(2 * time.Millisecond)

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
							Records: []Record{
								{Offset: 0, Time: t0, Key: nil, Value: String("msg-0")},
								{Offset: 1, Time: t1, Key: nil, Value: String("msg-1")},
								{Offset: 2, Time: t2, Key: Bytes([]byte{1}), Value: String("msg-2")},
							},
						},
					},
				},
			},
		},
	})
}

func BenchmarkFetchResponse(b *testing.B) {
	t0 := time.Now().Truncate(time.Millisecond)
	t1 := t0.Add(1 * time.Millisecond)
	t2 := t0.Add(2 * time.Millisecond)

	benchmarkResponse(b, v0, &FetchResponse{
		Topics: []FetchResponseTopic{
			{
				Topic: "topic-1",
				Partitions: []FetchResponsePartition{
					{
						Partition:     1,
						HighWatermark: 1000,
						RecordSet: RecordSet{
							Version: 1,
							Records: []Record{
								{Offset: 0, Time: t0, Key: nil, Value: String("msg-0")},
								{Offset: 1, Time: t1, Key: nil, Value: String("msg-1")},
								{Offset: 2, Time: t2, Key: Bytes([]byte{1}), Value: String("msg-2")},
							},
						},
					},
				},
			},
		},
	})
}
