package records_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sort"
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
			offset := int64(0)

			for k := 0; k < numRecordBatches; k++ {
				numRecords := g.RecordsPerRecordBatch.Generate(prng)
				records := make(RecordBatch, numRecords)

				for n := range records {
					records[n] = Record{
						Offset: offset,
						Time:   now,
						Key:    GenerateRandomBytes(prng, g.RecordKeySize.Generate(prng)),
						Value:  GenerateRandomBytes(prng, g.RecordValueSize.Generate(prng)),
					}
					offset++
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

func (s Server) Topics() []string {
	topics := make([]string, 0, len(s))
	for topic := range s {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}

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
	for _, records := range p {
		if records.Contains(offset) {
			return &protocol.RecordBatch{
				Attributes:           0,
				PartitionLeaderEpoch: -1,
				BaseOffset:           records.BaseOffset(),
				LastOffsetDelta:      records.LastOffsetDelta(),
				FirstTimestamp:       time.Time{},
				MaxTimestamp:         time.Time{},
				ProducerID:           -1,
				ProducerEpoch:        -1,
				BaseSequence:         -1,
				NumRecords:           records.NumRecords(),
				Records:              newRecordReader(records),
			}
		}
	}
	return nil
}

type RecordBatch []Record

func (r RecordBatch) Contains(offset int64) bool {
	return offset >= r.BaseOffset() && offset < r.LastOffset()
}

func (r RecordBatch) BaseOffset() int64 {
	if len(r) > 0 {
		return r[0].Offset
	}
	return -1
}

func (r RecordBatch) LastOffset() int64 {
	if len(r) > 0 {
		return r[len(r)-1].Offset + 1
	}
	return -1
}

func (r RecordBatch) LastOffsetDelta() int32 {
	return int32(r.LastOffset() - r.BaseOffset())
}

func (r RecordBatch) NumRecords() int32 {
	return int32(len(r))
}

type Record struct {
	Offset  int64
	Time    time.Time
	Key     []byte
	Value   []byte
	Headers []kafka.Header
}

func newRecordReader(records []Record) kafka.RecordReader {
	kafkaRecords := make([]kafka.Record, len(records))

	for i, r := range records {
		kafkaRecords[i] = kafka.Record{
			Offset:  r.Offset,
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

		stats := cache.Stats()
		if stats.Hits == 0 {
			t.Errorf("no cache hits: %+v\n", stats)
		}
		logCacheStats(t, stats)
	})
}

func testRecordsRandomAccess(t *testing.T, server Server, client *kafka.Client) {
	ctx := context.Background()

	for topic, partitions := range server {
		for partition, recordBatches := range partitions {
			for _, recordBatch := range recordBatches {
				for i := range recordBatch {
					offset := recordBatch.BaseOffset() + int64(i)
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
						newRecordReader(recordBatch),
					)
					r.Records.Close()
					if !ok {
						return
					}
					offset++
				}
			}
		}
	}
}

func BenchmarkRecordsRandomAccess(b *testing.B) {
	generator := ServerGenerator{
		NumTopics: 10,
		TopicNameSize: Range{
			Min: 10,
			Max: 20,
		},
		PartitionsPerTopic: Range{
			Min: 1,
			Max: 3,
		},
		RecordBatchesPerPartition: Range{
			Min: 1,
			Max: 10,
		},
		RecordsPerRecordBatch: Range{
			Min: 1,
			Max: 10,
		},
		RecordKeySize: Range{
			Min: 0,
			Max: 256,
		},
		RecordValueSize: Range{
			Min: 0,
			Max: 4096,
		},
	}

	// tmp, err := os.MkdirTemp("", "kafka-go.records.*")
	// if err != nil {
	// 	b.Fatal(err)
	// }
	// defer os.RemoveAll(tmp)
	// storage := records.MountPoint(tmp)
	storage := records.NewStorage()

	server := generator.Generate(rand.New(rand.NewSource(0)))
	cache := &records.Cache{
		SizeLimit: 8 * 1024 * 1024,
		Storage:   storage,
		Transport: server,
	}

	topics := server.Topics()
	numPartitions := 0
	numRecordBatches := 0
	numRecords := 0
	for _, topic := range server {
		numPartitions += len(topic)
		for _, partition := range topic {
			numRecordBatches += len(partition)
			for _, recordBatch := range partition {
				numRecords += len(recordBatch)
			}
		}
	}

	prng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx := context.Background()
	req := &kafka.FetchRequest{
		MinBytes: 1,
		MaxBytes: 1,
	}

	client := &kafka.Client{
		Addr:      kafka.TCP("whatever"),
		Transport: cache,
	}

	b.Logf("topics:%d partitions:%d batches:%d records:%d", len(topics), numPartitions, numRecordBatches, numRecords)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		topicName := topics[prng.Intn(len(topics))]
		topic := server[topicName]

		partitionID := prng.Intn(len(topic))
		partition := topic[partitionID]

		recordBatch := partition[prng.Intn(len(partition))]
		fetchOffset := recordBatch.BaseOffset() + prng.Int63n(int64(len(recordBatch)))

		req.Topic = topicName
		req.Partition = partitionID
		req.Offset = fetchOffset

		r, err := client.Fetch(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		r.Records.Close()
	}

	stats := cache.Stats()
	logCacheStats(b, stats)

	if stats.Inserts > int64(numRecordBatches) {
		b.Fatalf("too many entries were inserted in the cache: %d > %d", stats.Inserts, numRecordBatches)
	}
}

func logCacheStats(t testing.TB, stats records.CacheStats) {
	t.Logf("size:%dB inserts:%d evictions:%d hits:%d/%d (%.2f%%)",
		stats.Size,
		stats.Inserts,
		stats.Evictions,
		stats.Hits,
		stats.Lookups,
		100*float64(stats.Hits)/float64(stats.Lookups))
}
