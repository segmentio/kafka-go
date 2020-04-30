package kafka

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go/compress"
	"github.com/segmentio/kafka-go/protocol"
	prod "github.com/segmentio/kafka-go/protocol/produce"
)

// ProduceRequest represents a request sent to a kafka broker to produce records
// to a topic partition.
type ProduceRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	// The topic to produce the records to.
	Topic string

	// The partition to produce the records to.
	Partition int

	// The level of required acknowledgements to ask the kafka broker for.
	RequiredAcks int

	// An optional transaction id when producing to the kafka broker is part of
	// a transaction.
	TransactionalID string

	// The sequence of records to produce to the topic partition.
	Records RecordSet

	// An optional compression algorithm to apply to the batch of records sent
	// to the kafka broker.
	Compression compress.Compression

	// When set to true, produce requests sent by this client will use message
	// sets instead of record sets (messages v1). This is required if publishing
	// to a kafka broker in version 0.10.x.x.
	MessageSet bool
}

// ProduceResponse represents a response from a kafka broker to a produce
// request.
type ProduceResponse struct {
	// The amount of time that the broker throttled the request.
	Throttle time.Duration

	// An error that may have occured while attempting to produce the records.
	//
	// The error contains both the kafka error code, and an error message
	// returned by the kafka broker. Programs may use the standard errors.Is
	// function to test the error against kafka error codes.
	Error error

	// Offset of the first record that was written to the topic partition.
	//
	// This field will be zero if the kafka broker did no support the Produce
	// API in version 3 or above.
	BaseOffset int64

	// Time at which the broker wrote the records to the topic partition.
	//
	// This field will be zero if the kafka broker did no support the Produce
	// API in version 2 or above.
	LogAppendTime time.Time

	// First offset in the topic partition that the records were written to.
	//
	// This field will be zero if the kafka broker did no support the Produce
	// API in version 5 or above (or if the first offset is zero).
	LogStartOffset int64

	// If errors occured writing specific records, they will be reported in this
	// map.
	//
	// This field will be zero if the kafka broker did no support the Produce
	// API in version 8 or above.
	RecordErrors map[int]error
}

// Produce sends a produce request to a kafka broker and returns the response.
func (c *Client) Produce(ctx context.Context, req *ProduceRequest) (*ProduceResponse, error) {
	defer req.Records.Close()

	records := make([]protocol.Record, 0, 100)
	for rec := req.Records.Next(); rec != nil; rec = req.Records.Next() {
		records = append(records, protocol.Record{
			Time:    rec.Time(),
			Key:     rec.Key(),
			Value:   rec.Value(),
			Headers: rec.Headers(),
		})
	}

	for i := range records {
		r := &records[i]
		r.Offset = int64(i)
	}

	for _, r := range records {
		fmt.Println(r)
	}

	version := int8(2)
	if req.MessageSet {
		version = 1
	}

	attributes := protocol.Attributes(req.Compression) & 0x7

	m, err := c.roundTrip(ctx, req.Addr, &prod.Request{
		TransactionalID: req.TransactionalID,
		Acks:            int16(req.RequiredAcks),
		Timeout:         c.timeoutMs(ctx),
		Topics: []prod.RequestTopic{{
			Topic: req.Topic,
			Partitions: []prod.RequestPartition{{
				Partition: int32(req.Partition),
				RecordSet: protocol.RecordSet{
					Version:              version,
					Attributes:           attributes,
					PartitionLeaderEpoch: -1,
					BaseOffset:           0,
					ProducerID:           -1,
					ProducerEpoch:        -1,
					BaseSequence:         -1,
					Records:              records,
				},
			}},
		}},
	})

	if err != nil {
		return nil, fmt.Errorf("kafka.(*Client).Produce: %w", err)
	}

	res := m.(*prod.Response)
	if len(res.Topics) == 0 {
		return nil, fmt.Errorf("kafka.(*Client).Produce: %w", errNoTopics)
	}
	topic := &res.Topics[0]
	if len(topic.Partitions) == 0 {
		return nil, fmt.Errorf("kafka.(*Client).Produce: %w", errNoPartitions)
	}
	part := &topic.Partitions[0]

	ret := &ProduceResponse{
		Throttle:       duration(res.ThrottleTimeMs),
		Error:          makeError(part.ErrorCode, part.ErrorMessage),
		BaseOffset:     part.BaseOffset,
		LogAppendTime:  timestampToTime(part.LogAppendTime),
		LogStartOffset: part.LogStartOffset,
	}

	if len(part.RecordErrors) != 0 {
		ret.RecordErrors = make(map[int]error, len(part.RecordErrors))

		for _, recErr := range part.RecordErrors {
			ret.RecordErrors[int(recErr.BatchIndex)] = errors.New(recErr.BatchIndexErrorMessage)
		}
	}

	return ret, nil
}

var (
	errNoTopics     = errors.New("the kafka broker returned no topics in the produce response")
	errNoPartitions = errors.New("the kafka broker returned no partitions in the produce response")
)

type produceRequestV2 struct {
	RequiredAcks int16
	Timeout      int32
	Topics       []produceRequestTopicV2
}

func (r produceRequestV2) size() int32 {
	return 2 + 4 + sizeofArray(len(r.Topics), func(i int) int32 { return r.Topics[i].size() })
}

func (r produceRequestV2) writeTo(wb *writeBuffer) {
	wb.writeInt16(r.RequiredAcks)
	wb.writeInt32(r.Timeout)
	wb.writeArray(len(r.Topics), func(i int) { r.Topics[i].writeTo(wb) })
}

type produceRequestTopicV2 struct {
	TopicName  string
	Partitions []produceRequestPartitionV2
}

func (t produceRequestTopicV2) size() int32 {
	return sizeofString(t.TopicName) +
		sizeofArray(len(t.Partitions), func(i int) int32 { return t.Partitions[i].size() })
}

func (t produceRequestTopicV2) writeTo(wb *writeBuffer) {
	wb.writeString(t.TopicName)
	wb.writeArray(len(t.Partitions), func(i int) { t.Partitions[i].writeTo(wb) })
}

type produceRequestPartitionV2 struct {
	Partition      int32
	MessageSetSize int32
	MessageSet     messageSet
}

func (p produceRequestPartitionV2) size() int32 {
	return 4 + 4 + p.MessageSet.size()
}

func (p produceRequestPartitionV2) writeTo(wb *writeBuffer) {
	wb.writeInt32(p.Partition)
	wb.writeInt32(p.MessageSetSize)
	p.MessageSet.writeTo(wb)
}

type produceResponseV2 struct {
	ThrottleTime int32
	Topics       []produceResponseTopicV2
}

func (r produceResponseV2) size() int32 {
	return 4 + sizeofArray(len(r.Topics), func(i int) int32 { return r.Topics[i].size() })
}

func (r produceResponseV2) writeTo(wb *writeBuffer) {
	wb.writeInt32(r.ThrottleTime)
	wb.writeArray(len(r.Topics), func(i int) { r.Topics[i].writeTo(wb) })
}

type produceResponseTopicV2 struct {
	TopicName  string
	Partitions []produceResponsePartitionV2
}

func (t produceResponseTopicV2) size() int32 {
	return sizeofString(t.TopicName) +
		sizeofArray(len(t.Partitions), func(i int) int32 { return t.Partitions[i].size() })
}

func (t produceResponseTopicV2) writeTo(wb *writeBuffer) {
	wb.writeString(t.TopicName)
	wb.writeArray(len(t.Partitions), func(i int) { t.Partitions[i].writeTo(wb) })
}

type produceResponsePartitionV2 struct {
	Partition int32
	ErrorCode int16
	Offset    int64
	Timestamp int64
}

func (p produceResponsePartitionV2) size() int32 {
	return 4 + 2 + 8 + 8
}

func (p produceResponsePartitionV2) writeTo(wb *writeBuffer) {
	wb.writeInt32(p.Partition)
	wb.writeInt16(p.ErrorCode)
	wb.writeInt64(p.Offset)
	wb.writeInt64(p.Timestamp)
}

func (p *produceResponsePartitionV2) readFrom(r *bufio.Reader, sz int) (remain int, err error) {
	if remain, err = readInt32(r, sz, &p.Partition); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &p.ErrorCode); err != nil {
		return
	}
	if remain, err = readInt64(r, remain, &p.Offset); err != nil {
		return
	}
	if remain, err = readInt64(r, remain, &p.Timestamp); err != nil {
		return
	}
	return
}

type produceResponsePartitionV7 struct {
	Partition   int32
	ErrorCode   int16
	Offset      int64
	Timestamp   int64
	StartOffset int64
}

func (p produceResponsePartitionV7) size() int32 {
	return 4 + 2 + 8 + 8 + 8
}

func (p produceResponsePartitionV7) writeTo(wb *writeBuffer) {
	wb.writeInt32(p.Partition)
	wb.writeInt16(p.ErrorCode)
	wb.writeInt64(p.Offset)
	wb.writeInt64(p.Timestamp)
	wb.writeInt64(p.StartOffset)
}

func (p *produceResponsePartitionV7) readFrom(r *bufio.Reader, sz int) (remain int, err error) {
	if remain, err = readInt32(r, sz, &p.Partition); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &p.ErrorCode); err != nil {
		return
	}
	if remain, err = readInt64(r, remain, &p.Offset); err != nil {
		return
	}
	if remain, err = readInt64(r, remain, &p.Timestamp); err != nil {
		return
	}
	if remain, err = readInt64(r, remain, &p.StartOffset); err != nil {
		return
	}
	return
}
