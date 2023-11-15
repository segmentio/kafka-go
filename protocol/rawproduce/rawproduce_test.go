package rawproduce_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/prototest"
	"github.com/segmentio/kafka-go/protocol/rawproduce"
)

const (
	v0 = 0
	v3 = 3
	v5 = 5
)

func TestRawProduceRequest(t *testing.T) {
	t0 := time.Now().Truncate(time.Millisecond)
	t1 := t0.Add(1 * time.Millisecond)
	t2 := t0.Add(2 * time.Millisecond)

	prototest.TestRequestWithOverride(t, v0, &rawproduce.Request{
		Acks:    1,
		Timeout: 500,
		Topics: []rawproduce.RequestTopic{
			{
				Topic: "topic-1",
				Partitions: []rawproduce.RequestPartition{
					{
						Partition: 0,
						RecordSet: NewRawRecordSet(protocol.NewRecordReader(
							protocol.Record{Offset: 0, Time: t0, Key: nil, Value: nil},
						), 1, 0),
					},
					{
						Partition: 1,
						RecordSet: NewRawRecordSet(protocol.NewRecordReader(
							protocol.Record{Offset: 0, Time: t0, Key: nil, Value: prototest.String("msg-0")},
							protocol.Record{Offset: 1, Time: t1, Key: nil, Value: prototest.String("msg-1")},
							protocol.Record{Offset: 2, Time: t2, Key: prototest.Bytes([]byte{1}), Value: prototest.String("msg-2")},
						), 1, 0),
					},
				},
			},

			{
				Topic: "topic-2",
				Partitions: []rawproduce.RequestPartition{
					{
						Partition: 0,
						RecordSet: NewRawRecordSet(protocol.NewRecordReader(
							protocol.Record{Offset: 0, Time: t0, Key: nil, Value: prototest.String("msg-0")},
							protocol.Record{Offset: 1, Time: t1, Key: nil, Value: prototest.String("msg-1")},
							protocol.Record{Offset: 2, Time: t2, Key: prototest.Bytes([]byte{1}), Value: prototest.String("msg-2")},
						), 1, protocol.Gzip),
					},
				},
			},
		},
	})

	prototest.TestRequestWithOverride(t, v3, &rawproduce.Request{
		TransactionalID: "1234",
		Acks:            1,
		Timeout:         500,
		Topics: []rawproduce.RequestTopic{
			{
				Topic: "topic-1",
				Partitions: []rawproduce.RequestPartition{
					{
						Partition: 0,
						RecordSet: NewRawRecordSet(protocol.NewRecordReader(
							protocol.Record{Offset: 0, Time: t0, Key: nil, Value: nil},
						), 1, 0),
					},
					{
						Partition: 1,
						RecordSet: NewRawRecordSet(protocol.NewRecordReader(
							protocol.Record{Offset: 0, Time: t0, Key: nil, Value: prototest.String("msg-0")},
							protocol.Record{Offset: 1, Time: t1, Key: nil, Value: prototest.String("msg-1")},
							protocol.Record{Offset: 2, Time: t2, Key: prototest.Bytes([]byte{1}), Value: prototest.String("msg-2")},
						), 1, 0),
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

	prototest.TestRequestWithOverride(t, v5, &rawproduce.Request{
		TransactionalID: "1234",
		Acks:            1,
		Timeout:         500,
		Topics: []rawproduce.RequestTopic{
			{
				Topic: "topic-1",
				Partitions: []rawproduce.RequestPartition{
					{
						Partition: 1,
						RecordSet: NewRawRecordSet(protocol.NewRecordReader(
							protocol.Record{Offset: 0, Time: t0, Key: nil, Value: prototest.String("msg-0"), Headers: headers},
							protocol.Record{Offset: 1, Time: t1, Key: nil, Value: prototest.String("msg-1")},
							protocol.Record{Offset: 2, Time: t2, Key: prototest.Bytes([]byte{1}), Value: prototest.String("msg-2")},
						), 2, 0),
					},
				},
			},

			{
				Topic: "topic-2",
				Partitions: []rawproduce.RequestPartition{
					{
						Partition: 1,
						RecordSet: NewRawRecordSet(protocol.NewRecordReader(
							protocol.Record{Offset: 0, Time: t0, Key: nil, Value: prototest.String("msg-0"), Headers: headers},
							protocol.Record{Offset: 1, Time: t1, Key: nil, Value: prototest.String("msg-1")},
							protocol.Record{Offset: 2, Time: t2, Key: prototest.Bytes([]byte{1}), Value: prototest.String("msg-2")},
						), 2, protocol.Snappy),
					},
				},
			},
		},
	})
}

func NewRawRecordSet(reader protocol.RecordReader, version int8, attr protocol.Attributes) protocol.RawRecordSet {
	rs := protocol.RecordSet{Version: version, Attributes: attr, Records: reader}
	buf := &bytes.Buffer{}
	rs.WriteTo(buf)

	return protocol.RawRecordSet{
		Reader: buf,
	}
}

func BenchmarkProduceRequest(b *testing.B) {
	t0 := time.Now().Truncate(time.Millisecond)
	t1 := t0.Add(1 * time.Millisecond)
	t2 := t0.Add(2 * time.Millisecond)

	prototest.BenchmarkRequest(b, v3, &rawproduce.Request{
		TransactionalID: "1234",
		Acks:            1,
		Timeout:         500,
		Topics: []rawproduce.RequestTopic{
			{
				Topic: "topic-1",
				Partitions: []rawproduce.RequestPartition{
					{
						Partition: 0,
						RecordSet: NewRawRecordSet(protocol.NewRecordReader(
							protocol.Record{Offset: 0, Time: t0, Key: nil, Value: nil},
						), 1, 0),
					},
					{
						Partition: 1,
						RecordSet: NewRawRecordSet(protocol.NewRecordReader(
							protocol.Record{Offset: 0, Time: t0, Key: nil, Value: prototest.String("msg-0")},
							protocol.Record{Offset: 1, Time: t1, Key: nil, Value: prototest.String("msg-1")},
							protocol.Record{Offset: 2, Time: t2, Key: prototest.Bytes([]byte{1}), Value: prototest.String("msg-2")},
						), 1, 0),
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

	prototest.BenchmarkRequest(b, v5, &rawproduce.Request{
		TransactionalID: "1234",
		Acks:            1,
		Timeout:         500,
		Topics: []rawproduce.RequestTopic{
			{
				Topic: "topic-1",
				Partitions: []rawproduce.RequestPartition{
					{
						Partition: 1,
						RecordSet: NewRawRecordSet(protocol.NewRecordReader(
							protocol.Record{Offset: 0, Time: t0, Key: nil, Value: prototest.String("msg-0"), Headers: headers},
							protocol.Record{Offset: 1, Time: t1, Key: nil, Value: prototest.String("msg-1")},
							protocol.Record{Offset: 2, Time: t2, Key: prototest.Bytes([]byte{1}), Value: prototest.String("msg-2")},
						), 2, 0),
					},
				},
			},
		},
	})
}
