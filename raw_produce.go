package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/rawproduce"
)

// RawProduceRequest represents a request sent to a kafka broker to produce records
// to a topic partition.
type RawProduceRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	// The topic to produce the records to.
	Topic string

	// The partition to produce the records to.
	Partition int

	// The level of required acknowledgements to ask the kafka broker for.
	RequiredAcks RequiredAcks

	// The message format version used when encoding the records.
	//
	// By default, the client automatically determine which version should be
	// used based on the version of the Produce API supported by the server.
	MessageVersion int

	// An optional transaction id when producing to the kafka broker is part of
	// a transaction.
	TransactionalID string

	// The sequence of records to produce to the topic partition.
	RawRecords protocol.RawRecordSet
}

// RawProduceResponse represents a response from a kafka broker to a produce
// request.
type RawProduceResponse struct {
	// The amount of time that the broker throttled the request.
	Throttle time.Duration

	// An error that may have occurred while attempting to produce the records.
	//
	// The error contains both the kafka error code, and an error message
	// returned by the kafka broker. Programs may use the standard errors.Is
	// function to test the error against kafka error codes.
	Error error

	// Offset of the first record that was written to the topic partition.
	//
	// This field will be zero if the kafka broker did not support Produce API
	// version 3 or above.
	BaseOffset int64

	// Time at which the broker wrote the records to the topic partition.
	//
	// This field will be zero if the kafka broker did not support Produce API
	// version 2 or above.
	LogAppendTime time.Time

	// First offset in the topic partition that the records were written to.
	//
	// This field will be zero if the kafka broker did not support Produce
	// API version 5 or above (or if the first offset is zero).
	LogStartOffset int64

	// If errors occurred writing specific records, they will be reported in
	// this map.
	//
	// This field will always be empty if the kafka broker did not support the
	// Produce API in version 8 or above.
	RecordErrors map[int]error
}

// RawProduce sends a rawproduce request to a kafka broker and returns the response.
//
// If the request contained no records, an error wrapping protocol.ErrNoRecord
// is returned.
//
// When the request is configured with RequiredAcks=none, both the response and
// the error will be nil on success.
func (c *Client) RawProduce(ctx context.Context, req *RawProduceRequest) (*RawProduceResponse, error) {
	m, err := c.roundTrip(ctx, req.Addr, &rawproduce.RawRequest{
		TransactionalID: req.TransactionalID,
		Acks:            int16(req.RequiredAcks),
		Timeout:         c.timeoutMs(ctx, defaultProduceTimeout),
		Topics: []rawproduce.RequestTopic{{
			Topic: req.Topic,
			Partitions: []rawproduce.RequestPartition{{
				Partition: int32(req.Partition),
				RecordSet: req.RawRecords,
			}},
		}},
	})

	switch {
	case err == nil:
	case errors.Is(err, protocol.ErrNoRecord):
		return new(RawProduceResponse), nil
	default:
		return nil, fmt.Errorf("kafka.(*Client).Produce: %w", err)
	}

	if req.RequiredAcks == RequireNone {
		return nil, nil
	}

	res := m.(*rawproduce.RawResponse)
	if len(res.Topics) == 0 {
		return nil, fmt.Errorf("kafka.(*Client).Produce: %w", protocol.ErrNoTopic)
	}
	topic := &res.Topics[0]
	if len(topic.Partitions) == 0 {
		return nil, fmt.Errorf("kafka.(*Client).Produce: %w", protocol.ErrNoPartition)
	}
	partition := &topic.Partitions[0]

	ret := &RawProduceResponse{
		Throttle:       makeDuration(res.ThrottleTimeMs),
		Error:          makeError(partition.ErrorCode, partition.ErrorMessage),
		BaseOffset:     partition.BaseOffset,
		LogAppendTime:  makeTime(partition.LogAppendTime),
		LogStartOffset: partition.LogStartOffset,
	}

	if len(partition.RecordErrors) != 0 {
		ret.RecordErrors = make(map[int]error, len(partition.RecordErrors))

		for _, recErr := range partition.RecordErrors {
			ret.RecordErrors[int(recErr.BatchIndex)] = errors.New(recErr.BatchIndexErrorMessage)
		}
	}

	return ret, nil
}
