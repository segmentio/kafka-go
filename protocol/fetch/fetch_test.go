package fetch_test

import (
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/fetch"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0  = 0
	v11 = 11
)

func TestFetchRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &fetch.Request{
		ReplicaID:   -1,
		MaxWaitTime: 500,
		MinBytes:    1024,
		Topics: []fetch.RequestTopic{
			{
				Topic: "topic-1",
				Partitions: []fetch.RequestPartition{
					{
						Partition:         1,
						FetchOffset:       2,
						PartitionMaxBytes: 1024,
					},
				},
			},
		},
	})

	// KIP-392: round-trip a v11 request with RackID set to make sure the
	// rack id is encoded and decoded unchanged.
	prototest.TestRequest(t, v11, &fetch.Request{
		ReplicaID:    -1,
		MaxWaitTime:  500,
		MinBytes:     1024,
		MaxBytes:     1 << 20,
		SessionID:    -1,
		SessionEpoch: -1,
		Topics: []fetch.RequestTopic{
			{
				Topic: "topic-1",
				Partitions: []fetch.RequestPartition{
					{
						Partition:          1,
						CurrentLeaderEpoch: -1,
						FetchOffset:        2,
						LogStartOffset:     -1,
						PartitionMaxBytes:  1024,
					},
				},
			},
		},
		RackID: "rack-nl",
	})
}

func TestFetchResponse(t *testing.T) {
	t0 := time.Now().Truncate(time.Millisecond)
	t1 := t0.Add(1 * time.Millisecond)
	t2 := t0.Add(2 * time.Millisecond)

	prototest.TestResponse(t, v0, &fetch.Response{
		Topics: []fetch.ResponseTopic{
			{
				Topic: "topic-1",
				Partitions: []fetch.ResponsePartition{
					{
						Partition:     1,
						HighWatermark: 1000,
						RecordSet: protocol.RecordSet{
							Version: 1,
							Records: protocol.NewRecordReader(
								protocol.Record{Offset: 0, Time: t0, Key: nil, Value: prototest.String("msg-0")},
								protocol.Record{Offset: 1, Time: t1, Key: nil, Value: prototest.String("msg-1")},
								protocol.Record{Offset: 2, Time: t2, Key: prototest.Bytes([]byte{1}), Value: prototest.String("msg-2")},
							),
						},
					},
				},
			},
		},
	})

	headers := []protocol.Header{
		{Key: "key-1", Value: []byte("value-1")},
		{Key: "key-2", Value: []byte("value-2")},
		{Key: "key-3", Value: []byte("value-3")},
	}

	prototest.TestResponse(t, v11, &fetch.Response{
		Topics: []fetch.ResponseTopic{
			{
				Topic: "topic-1",
				Partitions: []fetch.ResponsePartition{
					{
						Partition:            1,
						HighWatermark:        1000,
						PreferredReadReplica: 42,
						RecordSet: protocol.RecordSet{
							Version: 2,
							Records: protocol.NewRecordReader(
								protocol.Record{Offset: 0, Time: t0, Key: nil, Value: prototest.String("msg-0"), Headers: headers},
								protocol.Record{Offset: 1, Time: t1, Key: nil, Value: prototest.String("msg-1")},
								protocol.Record{Offset: 2, Time: t2, Key: prototest.Bytes([]byte{1}), Value: prototest.String("msg-2")},
							),
						},
					},
				},
			},
		},
	})

	// KIP-392: when no preferred replica exists the broker sends -1.
	// Make sure that value round-trips so the client doesn't accidentally
	// flip it to 0 (which is a valid broker id).
	prototest.TestResponse(t, v11, &fetch.Response{
		Topics: []fetch.ResponseTopic{
			{
				Topic: "topic-1",
				Partitions: []fetch.ResponsePartition{
					{
						Partition:            1,
						HighWatermark:        1000,
						PreferredReadReplica: -1,
						RecordSet: protocol.RecordSet{
							Version: 2,
							Records: protocol.NewRecordReader(
								protocol.Record{Offset: 0, Time: t0, Key: nil, Value: prototest.String("msg-0")},
							),
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

	prototest.BenchmarkResponse(b, v0, &fetch.Response{
		Topics: []fetch.ResponseTopic{
			{
				Topic: "topic-1",
				Partitions: []fetch.ResponsePartition{
					{
						Partition:     1,
						HighWatermark: 1000,
						RecordSet: protocol.RecordSet{
							Version: 1,
							Records: protocol.NewRecordReader(
								protocol.Record{Offset: 0, Time: t0, Key: nil, Value: prototest.String("msg-0")},
								protocol.Record{Offset: 1, Time: t1, Key: nil, Value: prototest.String("msg-1")},
								protocol.Record{Offset: 2, Time: t2, Key: prototest.Bytes([]byte{1}), Value: prototest.String("msg-2")},
							),
						},
					},
				},
			},
		},
	})

	headers := []protocol.Header{
		{Key: "key-1", Value: []byte("value-1")},
		{Key: "key-2", Value: []byte("value-2")},
		{Key: "key-3", Value: []byte("value-3")},
	}

	prototest.BenchmarkResponse(b, v11, &fetch.Response{
		Topics: []fetch.ResponseTopic{
			{
				Topic: "topic-1",
				Partitions: []fetch.ResponsePartition{
					{
						Partition:     1,
						HighWatermark: 1000,
						RecordSet: protocol.RecordSet{
							Version: 2,
							Records: protocol.NewRecordReader(
								protocol.Record{Offset: 0, Time: t0, Key: nil, Value: prototest.String("msg-0"), Headers: headers},
								protocol.Record{Offset: 1, Time: t1, Key: nil, Value: prototest.String("msg-1")},
								protocol.Record{Offset: 2, Time: t2, Key: prototest.Bytes([]byte{1}), Value: prototest.String("msg-2")},
							),
						},
					},
				},
			},
		},
	})
}
