package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go/protocol/createpartitions"
)

// CreatePartitionsRequest represents a request sent to a kafka broker to create
// and update topic parititions.
type CreatePartitionsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	// List of topics to create and their configuration.
	Topics []TopicPartitionsConfig

	// When set to true, topics are not created but the configuration is
	// validated as if they were.
	ValidateOnly bool
}

// CreatePartitionsResponse represents a response from a kafka broker to a partitions
// creation request.
type CreatePartitionsResponse struct {
	// The amount of time that the broker throttled the request.
	//
	// This field will be zero if the kafka broker did no support the
	// CreatePartitions API in version 2 or above.
	Throttle time.Duration

	// Mapping of topic names to errors that occurred while attempting to create
	// the topics.
	//
	// The errors contain the kafka error code. Programs may use the standard
	// errors.Is function to test the error against kafka error codes.
	Errors map[string]error
}

// CreatePartitions sends a partitions creation request to a kafka broker and returns the
// response.
func (c *Client) CreatePartitions(ctx context.Context, req *CreatePartitionsRequest) (*CreatePartitionsResponse, error) {
	topics := make([]createpartitions.RequestTopic, len(req.Topics))

	for i, t := range req.Topics {
		topics[i] = createpartitions.RequestTopic{
			Name:        t.Name,
			Count:       t.Count,
			Assignments: t.assignments(),
		}
	}

	m, err := c.roundTrip(ctx, req.Addr, &createpartitions.Request{
		Topics:       topics,
		TimeoutMs:    c.timeoutMs(ctx, defaultCreatePartitionsTimeout),
		ValidateOnly: req.ValidateOnly,
	})

	if err != nil {
		return nil, fmt.Errorf("kafka.(*Client).CreatePartitions: %w", err)
	}

	res := m.(*createpartitions.Response)
	ret := &CreatePartitionsResponse{
		Throttle: makeDuration(res.ThrottleTimeMs),
		Errors:   make(map[string]error, len(res.Results)),
	}

	for _, t := range res.Results {
		ret.Errors[t.Name] = makeError(t.ErrorCode, t.ErrorMessage)
	}

	return ret, nil
}

type TopicPartitionsConfig struct {
	// Topic name
	Name string

	// Topic partition's count.
	Count int32

	// ReplicaAssignments among kafka brokers for this topic partitions.
	ReplicaAssignments []ReplicaAssignment
}

func (t *TopicPartitionsConfig) assignments() []createpartitions.RequestAssignment {
	if len(t.ReplicaAssignments) == 0 {
		return nil
	}
	assignments := make([]createpartitions.RequestAssignment, len(t.ReplicaAssignments))
	for i, a := range t.ReplicaAssignments {
		assignments[i] = createpartitions.RequestAssignment{
			BrokerIDs: a.brokerIDs(),
		}
	}
	return assignments
}
