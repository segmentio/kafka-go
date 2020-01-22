package protocol

import (
	"testing"
	"time"
)

func TestProduceRequest(t *testing.T) {
	t0 := time.Now().Truncate(time.Millisecond)
	t1 := t0.Add(1 * time.Millisecond)
	t2 := t0.Add(2 * time.Millisecond)

	testRequest(t, v0, &ProduceRequest{
		Acks:    1,
		Timeout: 500,
		Topics: []ProduceRequestTopic{
			{
				Topic: "topic-1",
				Partitions: []ProduceRequestPartition{
					{
						Partition: 0,
						RecordSet: RecordSet{
							Version: 1,
							Records: []Record{},
						},
					},
					{
						Partition: 1,
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

	testRequest(t, v3, &ProduceRequest{
		TransactionalID: "1234",
		Acks:            1,
		Timeout:         500,
		Topics: []ProduceRequestTopic{
			{
				Topic: "topic-1",
				Partitions: []ProduceRequestPartition{
					{
						Partition: 0,
						RecordSet: RecordSet{
							Version: 1,
						},
					},
					{
						Partition: 1,
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

func TestProduceResponse(t *testing.T) {
	testResponse(t, v0, &ProduceResponse{
		Topics: []ProduceResponseTopic{
			{
				Topic: "topic-1",
				Partitions: []ProduceResponsePartition{
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

	testResponse(t, v8, &ProduceResponse{
		Topics: []ProduceResponseTopic{
			{
				Topic: "topic-1",
				Partitions: []ProduceResponsePartition{
					{
						Partition:      0,
						ErrorCode:      0,
						BaseOffset:     42,
						LogAppendTime:  1e9,
						LogStartOffset: 10,
						RecordErrors:   []ProduceResponseError{},
					},
					{
						Partition: 1,
						ErrorCode: 1,
						RecordErrors: []ProduceResponseError{
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

	benchmarkRequest(b, v3, &ProduceRequest{
		TransactionalID: "1234",
		Acks:            1,
		Timeout:         500,
		Topics: []ProduceRequestTopic{
			{
				Topic: "topic-1",
				Partitions: []ProduceRequestPartition{
					{
						Partition: 0,
						RecordSet: RecordSet{
							Version: 1,
						},
					},
					{
						Partition: 1,
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
