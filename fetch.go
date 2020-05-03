package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	fetchAPI "github.com/segmentio/kafka-go/protocol/fetch"
)

// FetchRequest represents a request sent to a kafka broker to retrieve records
// from a topic partition.
type FetchRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	// Topic, partition, and offset to retrieve records from.
	Topic     string
	Partition int
	Offset    int64

	// Size and time limits of the response returned by the broker.
	MinBytes int64
	MaxBytes int64
	MaxWait  time.Duration

	// The isolation level for the request.
	//
	// Defaults to ReadUncommitted.
	//
	// This field requires the kafka broker to support the Fetch API in version
	// 4 or above (otherwise the value is ignored).
	IsolationLevel IsolationLevel
}

// FetchResponse represents a response from a kafka broker to a fetch request.
type FetchResponse struct {
	// The amount of time that the broker throttled the request.
	Throttle time.Duration

	// The topic and partition that the response came for (will match the values
	// in the request).
	Topic     string
	Partition int

	// Informations about the topic partition layout returned from the broker.
	//
	// LastStableOffset requires the kafka broker to support the Fetch API in
	// version 4 or above (otherwise the value is zero).
	//
	/// LogStartOffset requires the kafka broker to support the Fetch API in
	// version 5 or above (otherwise the value is zero).
	HighWatermark    int64
	LastStableOffset int64
	LogStartOffset   int64

	// An error that may have occured while attempting to fetch the records.
	//
	// The error contains both the kafka error code, and an error message
	// returned by the kafka broker. Programs may use the standard errors.Is
	// function to test the error against kafka error codes.
	Error error

	// The set of records returned in the response.
	//
	// The program is expected to call the RecordSet's Close method when it
	// finished reading the records.
	Records RecordSet

	// Indicates whether the response returned by the broker was transactional
	// or a control batch.
	Transactional bool
	ControlBatch  bool
}

// Fetch sends a fetch request to a kafka broker and returns the response.
func (c *Client) Fetch(ctx context.Context, req *FetchRequest) (*FetchResponse, error) {
	timeout := c.timeout(ctx)
	if req.MaxWait > 0 && req.MaxWait < timeout {
		timeout = req.MaxWait
	}

	m, err := c.roundTrip(ctx, req.Addr, &fetchAPI.Request{
		ReplicaID:      -1,
		MaxWaitTime:    milliseconds(timeout),
		MinBytes:       int32(req.MinBytes),
		MaxBytes:       int32(req.MaxBytes),
		IsolationLevel: int8(req.IsolationLevel),
		Topics: []fetchAPI.RequestTopic{{
			Topic: req.Topic,
			Partitions: []fetchAPI.RequestPartition{{
				Partition:         int32(req.Partition),
				FetchOffset:       req.Offset,
				PartitionMaxBytes: int32(req.MaxBytes),
			}},
		}},
	})

	if err != nil {
		return nil, fmt.Errorf("kafka.(*Client).Fetch: %w", err)
	}

	res := m.(*fetchAPI.Response)
	if len(res.Topics) == 0 {
		return nil, fmt.Errorf("kafka.(*Client).Fetch: %w", errNoTopics)
	}
	topic := &res.Topics[0]
	if len(topic.Partitions) == 0 {
		return nil, fmt.Errorf("kafka.(*Client).Fetch: %w", errNoPartitions)
	}
	partition := &topic.Partitions[0]

	ret := &FetchResponse{
		Throttle:      duration(res.ThrottleTimeMs),
		Topic:         topic.Topic,
		Partition:     int(partition.Partition),
		Error:         makeError(res.ErrorCode, ""),
		HighWatermark: partition.HighWatermark,
		Records: &fetchRecords{
			RecordSet: &partition.RecordSet,
		},
		Transactional: partition.RecordSet.Attributes.Transactional(),
		ControlBatch:  partition.RecordSet.Attributes.ControlBatch(),
	}

	if partition.ErrorCode != 0 {
		ret.Error = makeError(partition.ErrorCode, "")
	}

	return ret, nil
}

type fetchRecords struct {
	*protocol.RecordSet
	record fetchRecord
	index  int
}

func (r *fetchRecords) Next() Record {
	if r.index >= 0 && r.index < len(r.RecordSet.Records) {
		r.record.Record = r.RecordSet.Records[r.index]
		r.index++
		return &r.record
	}
	return nil
}

type fetchRecord struct{ protocol.Record }

func (r *fetchRecord) Offset() int64 { return r.Record.Offset }

func (r *fetchRecord) Time() time.Time { return r.Record.Time }

func (r *fetchRecord) Key() Bytes { return r.Record.Key }

func (r *fetchRecord) Value() Bytes { return r.Record.Value }

func (r *fetchRecord) Headers() []Header { return r.Record.Headers }

type fetchRequestV2 struct {
	ReplicaID   int32
	MaxWaitTime int32
	MinBytes    int32
	Topics      []fetchRequestTopicV2
}

func (r fetchRequestV2) size() int32 {
	return 4 + 4 + 4 + sizeofArray(len(r.Topics), func(i int) int32 { return r.Topics[i].size() })
}

func (r fetchRequestV2) writeTo(wb *writeBuffer) {
	wb.writeInt32(r.ReplicaID)
	wb.writeInt32(r.MaxWaitTime)
	wb.writeInt32(r.MinBytes)
	wb.writeArray(len(r.Topics), func(i int) { r.Topics[i].writeTo(wb) })
}

type fetchRequestTopicV2 struct {
	TopicName  string
	Partitions []fetchRequestPartitionV2
}

func (t fetchRequestTopicV2) size() int32 {
	return sizeofString(t.TopicName) +
		sizeofArray(len(t.Partitions), func(i int) int32 { return t.Partitions[i].size() })
}

func (t fetchRequestTopicV2) writeTo(wb *writeBuffer) {
	wb.writeString(t.TopicName)
	wb.writeArray(len(t.Partitions), func(i int) { t.Partitions[i].writeTo(wb) })
}

type fetchRequestPartitionV2 struct {
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
}

func (p fetchRequestPartitionV2) size() int32 {
	return 4 + 8 + 4
}

func (p fetchRequestPartitionV2) writeTo(wb *writeBuffer) {
	wb.writeInt32(p.Partition)
	wb.writeInt64(p.FetchOffset)
	wb.writeInt32(p.MaxBytes)
}

type fetchResponseV2 struct {
	ThrottleTime int32
	Topics       []fetchResponseTopicV2
}

func (r fetchResponseV2) size() int32 {
	return 4 + sizeofArray(len(r.Topics), func(i int) int32 { return r.Topics[i].size() })
}

func (r fetchResponseV2) writeTo(wb *writeBuffer) {
	wb.writeInt32(r.ThrottleTime)
	wb.writeArray(len(r.Topics), func(i int) { r.Topics[i].writeTo(wb) })
}

type fetchResponseTopicV2 struct {
	TopicName  string
	Partitions []fetchResponsePartitionV2
}

func (t fetchResponseTopicV2) size() int32 {
	return sizeofString(t.TopicName) +
		sizeofArray(len(t.Partitions), func(i int) int32 { return t.Partitions[i].size() })
}

func (t fetchResponseTopicV2) writeTo(wb *writeBuffer) {
	wb.writeString(t.TopicName)
	wb.writeArray(len(t.Partitions), func(i int) { t.Partitions[i].writeTo(wb) })
}

type fetchResponsePartitionV2 struct {
	Partition           int32
	ErrorCode           int16
	HighwaterMarkOffset int64
	MessageSetSize      int32
	MessageSet          messageSet
}

func (p fetchResponsePartitionV2) size() int32 {
	return 4 + 2 + 8 + 4 + p.MessageSet.size()
}

func (p fetchResponsePartitionV2) writeTo(wb *writeBuffer) {
	wb.writeInt32(p.Partition)
	wb.writeInt16(p.ErrorCode)
	wb.writeInt64(p.HighwaterMarkOffset)
	wb.writeInt32(p.MessageSetSize)
	p.MessageSet.writeTo(wb)
}
