package records_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/fetch"
	"github.com/segmentio/kafka-go/protocol/prototest"
	"github.com/segmentio/kafka-go/records"
)

type Range struct {
	Min int
	Max int
}

func (r *Range) Generate(prng *rand.Rand) int {
	return r.Min + prng.Intn(r.Max)
}

type ServerGenerator struct {
	NumTopics                 int
	TopicNameSize             Range
	PartitionsPerTopic        Range
	RecordBatchesPerPartition Range
	RecordsPerRecordBatch     Range
	RecordKeySize             Range
	RecordValueSize           Range
}

func (g *ServerGenerator) Generate(prng *rand.Rand) Server {
	server := make(Server, g.NumTopics)
	now := protocol.MakeTime(1645552808)

	for i := 0; i < g.NumTopics; i++ {
		numPartitions := g.PartitionsPerTopic.Generate(prng)
		topic := make(Topic, numPartitions)

		for j := 0; j < numPartitions; j++ {
			numRecordBatches := g.RecordBatchesPerPartition.Generate(prng)
			partition := make(Partition, numRecordBatches)

			for k := 0; k < numRecordBatches; k++ {
				numRecords := g.RecordsPerRecordBatch.Generate(prng)
				records := make(RecordBatch, numRecords)

				for n := range records {
					records[n] = Record{
						Time:  now,
						Key:   GenerateRandomBytes(prng, g.RecordKeySize.Generate(prng)),
						Value: GenerateRandomBytes(prng, g.RecordValueSize.Generate(prng)),
					}
					now = now.Add(time.Millisecond)
				}

				partition[k] = records
			}

			topic[j] = partition
		}

		topicName := GenerateRandomString(prng, g.TopicNameSize.Generate(prng))
		server[topicName] = topic
	}

	return server
}

func GenerateRandomString(prng *rand.Rand, n int) string {
	const characters = "0123456789abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = characters[prng.Intn(len(characters))]
	}
	return string(b)
}

func GenerateRandomBytes(prng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	io.ReadFull(prng, b)
	return b
}

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
				Attributes:           0,
				PartitionLeaderEpoch: -1,
				BaseOffset:           baseOffset,
				LastOffsetDelta:      int32(len(records)),
				FirstTimestamp:       time.Time{},
				MaxTimestamp:         time.Time{},
				ProducerID:           -1,
				ProducerEpoch:        -1,
				BaseSequence:         -1,
				NumRecords:           int32(len(records)),
				Records:              newRecordReader(baseOffset, records),
			}
		}
		baseOffset += int64(len(records))
	}

	return nil
}

type RecordBatch []Record

type Record struct {
	Time    time.Time
	Key     []byte
	Value   []byte
	Headers []kafka.Header
}

func newRecordReader(baseOffset int64, records []Record) kafka.RecordReader {
	kafkaRecords := make([]kafka.Record, len(records))

	for i, r := range records {
		kafkaRecords[i] = kafka.Record{
			Offset:  baseOffset + int64(i),
			Time:    r.Time,
			Key:     kafka.NewBytes(r.Key),
			Value:   kafka.NewBytes(r.Value),
			Headers: r.Headers,
		}
	}

	return kafka.NewRecordReader(kafkaRecords...)
}

func TestRecordsRandomAccess(t *testing.T) {
	generator := ServerGenerator{
		NumTopics: 10,
		TopicNameSize: Range{
			Min: 4,
			Max: 4,
		},
		PartitionsPerTopic: Range{
			Min: 1,
			Max: 3,
		},
		RecordBatchesPerPartition: Range{
			Min: 0,
			Max: 3,
		},
		RecordsPerRecordBatch: Range{
			Min: 0,
			Max: 4,
		},
		RecordKeySize: Range{
			Min: 0,
			Max: 2,
		},
		RecordValueSize: Range{
			Min: 0,
			Max: 2,
		},
	}

	prng := rand.New(rand.NewSource(0))
	server := generator.Generate(prng)

	t.Run("direct", func(t *testing.T) {
		testRecordsRandomAccess(t, server, &kafka.Client{
			Addr:      kafka.TCP("whatever"),
			Transport: server,
		})
	})

	t.Run("cache", func(t *testing.T) {
		cache := &records.Cache{
			SizeLimit: 1024 * 1024,
			Storage:   records.NewStorage(),
			Transport: server,
		}

		testRecordsRandomAccess(t, server, &kafka.Client{
			Addr:      kafka.TCP("whatever"),
			Transport: cache,
		})

		if stats := cache.Stats(); stats.Hits == 0 {
			t.Errorf("no cache hits: %+v\n", stats)
		}
	})
}

func testRecordsRandomAccess(t *testing.T, server Server, client *kafka.Client) {
	ctx := context.Background()

	for topic, partitions := range server {
		for partition, recordBatches := range partitions {
			baseOffset, offset := int64(0), int64(0)
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
						t.Fatalf("fetching record batch containing offset %d from partition %d of the %q topic: %v", offset, partition, topic, err)
					}

					if r.Topic != topic {
						t.Fatalf("topic of fetch response does not match the topic of the request: want=%q got=%q", topic, r.Topic)
					}

					if r.Partition != partition {
						t.Fatalf("partition of fetch response does not match the partition of the request: want=%d got=%d", partition, r.Partition)
					}

					if r.Error != nil {
						t.Fatalf("unexpected error in fetch response: %v", r.Error)
					}

					ok := prototest.AssertRecords(t,
						r.Records,
						newRecordReader(baseOffset, recordBatch),
					)
					r.Records.Close()
					if !ok {
						return
					}
					offset++
				}
				baseOffset = offset
			}
		}
	}
}
