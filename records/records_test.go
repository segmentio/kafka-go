package records_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/fetch"
	"github.com/segmentio/kafka-go/protocol/prototest"
	"github.com/segmentio/kafka-go/records"
)

type Server map[string]Topic

func (s Server) RoundTrip(ctx context.Context, addr net.Addr, req kafka.Request) (kafka.Response, error) {
	fetchReq, _ := req.(*fetch.Request)
	if fetchReq == nil {
		return nil, fmt.Errorf("unsupported request type: %T", req)
	}

	fetchRes := &fetch.Response{
		Topics: make([]fetch.ResponseTopic, len(fetchReq.Topics)),
	}

	for i, t := range fetchReq.Topics {
		resTopic := &fetchRes.Topics[i]
		resTopic.Topic = t.Topic
		resTopic.Partitions = make([]fetch.ResponsePartition, len(t.Partitions))

		topic := s[t.Topic]

		for j, p := range t.Partitions {
			resTopic.Partitions[j] = fetch.ResponsePartition{
				Partition: p.Partition,
				RecordSet: protocol.RecordSet{
					Version: 2,
					Records: protocol.NewRecordReader(),
				},
			}

			resPartition := &resTopic.Partitions[j]
			if p.Partition < 0 {
				resPartition.ErrorCode = int16(kafka.InvalidPartitionNumber)
				continue
			}
			if int(p.Partition) >= len(topic) {
				resPartition.ErrorCode = int16(kafka.UnknownTopicOrPartition)
				continue
			}
			topicPartition := topic[p.Partition]
			resPartition.LastStableOffset = topicPartition.LastStableOffset()
			resPartition.LogStartOffset = topicPartition.LogStartOffset()
			resPartition.HighWatermark = topicPartition.HighWatermark()

			recordBatch := topicPartition.Lookup(p.FetchOffset)
			if recordBatch == nil {
				resPartition.ErrorCode = int16(kafka.OffsetOutOfRange)
				continue
			}
			resPartition.RecordSet.Attributes = recordBatch.Attributes
			resPartition.RecordSet.Records = recordBatch
		}
	}

	return fetchRes, nil
}

type Topic []Partition

type Partition []RecordBatch

func (p Partition) LogStartOffset() int64 {
	return 0
}

func (p Partition) LastStableOffset() int64 {
	return p.HighWatermark() - 1
}

func (p Partition) HighWatermark() int64 {
	highWatermark := int64(0)
	for _, records := range p {
		highWatermark += int64(len(records))
	}
	return 0
}

func (p Partition) Lookup(offset int64) *protocol.RecordBatch {
	baseOffset := int64(0)

	for _, records := range p {
		if (baseOffset + int64(len(records))) > offset {
			return &protocol.RecordBatch{
				BaseOffset: baseOffset,
				NumRecords: int32(len(records)),
				Records:    newRecordReader(baseOffset, records),
			}
		}
		baseOffset += int64(len(records))
	}

	return nil
}

type RecordBatch []Record

type Record struct {
	Key     []byte
	Value   []byte
	Headers []kafka.Header
}

func newRecordReader(baseOffset int64, records []Record) kafka.RecordReader {
	kafkaRecords := make([]kafka.Record, len(records))

	for i, r := range records {
		kafkaRecords[i] = kafka.Record{
			Offset:  baseOffset + int64(i),
			Key:     kafka.NewBytes(r.Key),
			Value:   kafka.NewBytes(r.Value),
			Headers: r.Headers,
		}
	}

	return kafka.NewRecordReader(kafkaRecords...)
}

func TestRecordsRandomAccess(t *testing.T) {
	server := Server{
		"topic-A": {
			0: Partition{
				{{Value: []byte("A.0")}},
				{{Value: []byte("A.1")}, {Value: []byte("A.2")}},
				{{Value: []byte("A.3")}},
				{{Value: []byte("A.4")}},
			},
		},
		"topic-B": {
			0: Partition{
				{{Value: []byte("B.0")}, {Value: []byte("B.1")}, {Value: []byte("B.2")}},
			},
		},
		"topic-C": {
			0: Partition{
				{{Value: []byte("C.9")}},
			},
		},
	}

	t.Run("direct", func(t *testing.T) {
		testRecordsRandomAccess(t, server, &kafka.Client{
			Addr:      kafka.TCP("whatever"),
			Transport: server,
		})
	})

	t.Run("cache", func(t *testing.T) {
		testRecordsRandomAccess(t, server, &kafka.Client{
			Addr: kafka.TCP("whatever"),
			Transport: &records.Cache{
				SizeLimit: 4096,
				Storage:   records.NewStorage(),
				Transport: server,
			},
		})
	})
}

func testRecordsRandomAccess(t *testing.T, server Server, client *kafka.Client) {
	ctx := context.Background()

	for topic, partitions := range server {
		for partition, recordBatches := range partitions {
			baseOffset := int64(0)
			offset := int64(0)

			for _, recordBatch := range recordBatches {
				for range recordBatch {
					r, err := client.Fetch(ctx, &kafka.FetchRequest{
						Topic:     topic,
						Partition: partition,
						Offset:    offset,
						MinBytes:  1,
						MaxBytes:  1,
					})
					if err != nil {
						t.Errorf("fetching record batch containing offset %d from partition %d of the %q topic: %v", offset, partition, topic, err)
						continue
					}

					if r.Topic != topic {
						t.Errorf("topic of fetch response does not match the topic of the request: want=%q got=%q", topic, r.Topic)
					}

					if r.Partition != partition {
						t.Errorf("partition of fetch response does not match the partition of the request: want=%d got=%d", partition, r.Partition)
					}

					if r.Error != nil {
						t.Errorf("unexpected error in fetch response: %v", r.Error)
					}

					prototest.AssertRecords(t,
						r.Records,
						newRecordReader(baseOffset, recordBatch),
					)
					r.Records.Close()
					offset++
				}
				baseOffset = offset
			}
		}
	}
}
