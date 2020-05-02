package produce_test

import (
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/produce"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0 = 0
	v3 = 3
	v5 = 5
	v8 = 8
)

func TestProduceRequest(t *testing.T) {
	t0 := time.Now().Truncate(time.Millisecond)
	t1 := t0.Add(1 * time.Millisecond)
	t2 := t0.Add(2 * time.Millisecond)

	prototest.TestRequest(t, v0, &produce.Request{
		Acks:    1,
		Timeout: 500,
		Topics: []produce.RequestTopic{
			{
				Topic: "topic-1",
				Partitions: []produce.RequestPartition{
					{
						Partition: 0,
						RecordSet: protocol.RecordSet{
							Version: 0,
							Records: nil,
						},
					},
					{
						Partition: 1,
						RecordSet: protocol.RecordSet{
							Version: 1,
							Records: []protocol.Record{
								{Offset: 0, Time: t0, Key: nil, Value: protocol.String("msg-0")},
								{Offset: 1, Time: t1, Key: nil, Value: protocol.String("msg-1")},
								{Offset: 2, Time: t2, Key: protocol.Bytes([]byte{1}), Value: protocol.String("msg-2")},
							},
						},
					},
				},
			},

			{
				Topic: "topic-2",
				Partitions: []produce.RequestPartition{
					{
						Partition: 0,
						RecordSet: protocol.RecordSet{
							Version:    1,
							Attributes: protocol.Gzip,
							Records: []protocol.Record{
								{Offset: 0, Time: t0, Key: nil, Value: protocol.String("msg-0")},
								{Offset: 1, Time: t1, Key: nil, Value: protocol.String("msg-1")},
								{Offset: 2, Time: t2, Key: protocol.Bytes([]byte{1}), Value: protocol.String("msg-2")},
							},
						},
					},
				},
			},
		},
	})

	prototest.TestRequest(t, v3, &produce.Request{
		TransactionalID: "1234",
		Acks:            1,
		Timeout:         500,
		Topics: []produce.RequestTopic{
			{
				Topic: "topic-1",
				Partitions: []produce.RequestPartition{
					{
						Partition: 0,
						RecordSet: protocol.RecordSet{},
					},
					{
						Partition: 1,
						RecordSet: protocol.RecordSet{
							Version: 1,
							Records: []protocol.Record{
								{Offset: 0, Time: t0, Key: nil, Value: protocol.String("msg-0")},
								{Offset: 1, Time: t1, Key: nil, Value: protocol.String("msg-1")},
								{Offset: 2, Time: t2, Key: protocol.Bytes([]byte{1}), Value: protocol.String("msg-2")},
							},
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

	prototest.TestRequest(t, v5, &produce.Request{
		TransactionalID: "1234",
		Acks:            1,
		Timeout:         500,
		Topics: []produce.RequestTopic{
			{
				Topic: "topic-1",
				Partitions: []produce.RequestPartition{
					{
						Partition: 1,
						RecordSet: protocol.RecordSet{
							Version:              2,
							PartitionLeaderEpoch: 42,
							BaseOffset:           10,
							ProducerID:           1234567890,
							ProducerEpoch:        1234,
							BaseSequence:         5678,

							Records: []protocol.Record{
								{Offset: 11, Time: t0, Key: nil, Value: protocol.String("msg-0"), Headers: headers},
								{Offset: 12, Time: t1, Key: nil, Value: protocol.String("msg-1")},
								{Offset: 14, Time: t2, Key: protocol.Bytes([]byte{1}), Value: protocol.String("msg-2")},
							},
						},
					},
				},
			},

			{
				Topic: "topic-2",
				Partitions: []produce.RequestPartition{
					{
						Partition: 1,
						RecordSet: protocol.RecordSet{
							Version:    2,
							BaseOffset: 1,
							Attributes: protocol.Snappy,
							Records: []protocol.Record{
								{Offset: 0, Time: t0, Key: nil, Value: protocol.String("msg-0"), Headers: headers},
								{Offset: 1, Time: t1, Key: nil, Value: protocol.String("msg-1")},
								{Offset: 2, Time: t2, Key: protocol.Bytes([]byte{1}), Value: protocol.String("msg-2")},
							},
						},
					},
				},
			},
		},
	})
}

func TestProduceResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &produce.Response{
		Topics: []produce.ResponseTopic{
			{
				Topic: "topic-1",
				Partitions: []produce.ResponsePartition{
					{
						Partition:  0,
						ErrorCode:  0,
						BaseOffset: 0,
					},
					{
						Partition:  1,
						ErrorCode:  0,
						BaseOffset: 42,
					},
				},
			},
		},
	})

	prototest.TestResponse(t, v8, &produce.Response{
		Topics: []produce.ResponseTopic{
			{
				Topic: "topic-1",
				Partitions: []produce.ResponsePartition{
					{
						Partition:      0,
						ErrorCode:      0,
						BaseOffset:     42,
						LogAppendTime:  1e9,
						LogStartOffset: 10,
						RecordErrors:   []produce.ResponseError{},
					},
					{
						Partition: 1,
						ErrorCode: 1,
						RecordErrors: []produce.ResponseError{
							{BatchIndex: 1, BatchIndexErrorMessage: "message-1"},
							{BatchIndex: 2, BatchIndexErrorMessage: "message-2"},
							{BatchIndex: 3, BatchIndexErrorMessage: "message-3"},
						},
						ErrorMessage: "something went wrong",
					},
				},
			},
		},
	})
}

func BenchmarkProduceRequest(b *testing.B) {
	t0 := time.Now().Truncate(time.Millisecond)
	t1 := t0.Add(1 * time.Millisecond)
	t2 := t0.Add(2 * time.Millisecond)

	prototest.BenchmarkRequest(b, v3, &produce.Request{
		TransactionalID: "1234",
		Acks:            1,
		Timeout:         500,
		Topics: []produce.RequestTopic{
			{
				Topic: "topic-1",
				Partitions: []produce.RequestPartition{
					{
						Partition: 0,
						RecordSet: protocol.RecordSet{
							Version: 1,
						},
					},
					{
						Partition: 1,
						RecordSet: protocol.RecordSet{
							Version: 1,
							Records: []protocol.Record{
								{Offset: 0, Time: t0, Key: nil, Value: protocol.String("msg-0")},
								{Offset: 1, Time: t1, Key: nil, Value: protocol.String("msg-1")},
								{Offset: 2, Time: t2, Key: protocol.Bytes([]byte{1}), Value: protocol.String("msg-2")},
							},
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

	prototest.BenchmarkRequest(b, v5, &produce.Request{
		TransactionalID: "1234",
		Acks:            1,
		Timeout:         500,
		Topics: []produce.RequestTopic{
			{
				Topic: "topic-1",
				Partitions: []produce.RequestPartition{
					{
						Partition: 1,
						RecordSet: protocol.RecordSet{
							Version:              2,
							PartitionLeaderEpoch: 42,
							BaseOffset:           10,
							ProducerID:           1234567890,
							ProducerEpoch:        1234,
							BaseSequence:         5678,

							Records: []protocol.Record{
								{Offset: 11, Time: t0, Key: nil, Value: protocol.String("msg-0"), Headers: headers},
								{Offset: 12, Time: t1, Key: nil, Value: protocol.String("msg-1")},
								{Offset: 14, Time: t2, Key: protocol.Bytes([]byte{1}), Value: protocol.String("msg-2")},
							},
						},
					},
				},
			},
		},
	})
}
