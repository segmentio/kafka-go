package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/initproducerid"
)

// InitProducerIDRequest is the request structure for the InitProducerId function
type InitProducerIDRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	// The transactional id key.
	TransactionalID string

	// Time after which a transaction should time out
	TransactionTimeoutMs int32
}

// ProducerSession is
type ProducerSession struct {
	IsTransactional bool
	ProducerID      int64
	ProducerEpoch   int16
}

// InitProducerIDResponse is the response structure for the InitProducerId function
type InitProducerIDResponse struct {
	// The Transaction/Group Coordinator details
	Producer *ProducerSession

	// The amount of time that the broker throttled the request.
	Throttle time.Duration

	// An error that may have occurred while attempting to retrieve initProducerId
	//
	// The error contains both the kafka error code, and an error message
	// returned by the kafka broker.
	Error error
}

// InitProducerIdNotSupported is the message to display when InitProducerID is not supported
const InitProducerIdNotSupported string = "InitProdudcerId is not supported by this broker"

// InitProducerID sends a initProducerId request to a kafka broker and returns the
// response.
func (c *Client) InitProducerID(ctx context.Context, req *InitProducerIDRequest) (*InitProducerIDResponse, error) {

	supported, err := c.IsApiKeySupported(ctx, protocol.InitProducerId)
	if err != nil {
		return nil, err
	}
	if !supported {
		return nil, fmt.Errorf(InitProducerIdNotSupported)
	}

	m, err := c.roundTrip(ctx, req.Addr, &initproducerid.Request{
		TransactionalID:      req.TransactionalID,
		TransactionTimeoutMs: req.TransactionTimeoutMs,
	})

	if err != nil {
		return nil, fmt.Errorf("kafka.(*Client).InitProducerId: %w", err)
	}
	isTransactional := false
	if req.TransactionalID != "" {
		isTransactional = true
	}
	res := m.(*initproducerid.Response)
	producerSession := &ProducerSession{
		IsTransactional: isTransactional,
		ProducerID:      res.ProducerID,
		ProducerEpoch:   res.ProducerEpoch,
	}
	ret := &InitProducerIDResponse{
		Producer: producerSession,
		Throttle: makeDuration(res.ThrottleTimeMs),
		Error:    makeError(res.ErrorCode, ""),
	}
	return ret, ret.Error
}
