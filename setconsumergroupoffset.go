package kafka

import (
	"context"
	"fmt"
	"net"
	"time"
)

// SetConsumerGroupOffsetRequest is a request to explicitly set consumer group offsets.
type SetConsumerGroupOffsetRequest struct {
	// Addr is the address of the kafka broker to send the request to.
	Addr net.Addr

	// GroupID is the ID of the consumer group to set offsets for.
	GroupID string

	// GenerationID is the ID of the consumer group generation.
	// This is optional and can be set to 0 for most use cases.
	GenerationID int

	// MemberID is the ID of the group member submitting the offsets.
	// This is optional and can be set to empty string for most use cases.
	MemberID string

	// InstanceID is the ID of the group instance.
	// This is optional and can be set to empty string for most use cases.
	InstanceID string

	// Topics is a map of topic names to partition offsets to set.
	// The key is the topic name, and the value is a slice of offsets for each partition.
	Topics map[string][]SetOffset
}

// SetOffset represents setting an offset for a specific partition.
type SetOffset struct {
	Partition int
	Offset    int64
	Metadata  string
}

// SetConsumerGroupOffsetResponse is a response from setting consumer group offsets.
type SetConsumerGroupOffsetResponse struct {
	// The amount of time that the broker throttled the request.
	Throttle time.Duration

	// Set of topic partitions that the kafka broker has accepted offset sets for.
	Topics map[string][]SetOffsetPartition
}

// SetOffsetPartition represents the state of a single partition in responses
// to setting offsets.
type SetOffsetPartition struct {
	// ID of the partition.
	Partition int

	// An error that may have occurred while attempting to set consumer
	// group offsets for this partition.
	//
	// The error contains both the kafka error code, and an error message
	// returned by the kafka broker. Programs may use the standard errors.Is
	// function to test the error against kafka error codes.
	Error error
}

// SetConsumerGroupOffset sends a request to explicitly set consumer group offsets
// and returns the response. This function allows you to set offsets for a consumer
// group without being part of an active consumer group session.
//
// Note: This function creates a temporary consumer group session to obtain valid
// generation and member IDs required by the Kafka protocol. The session is closed
// immediately after setting the offsets.
func (c *Client) SetConsumerGroupOffset(ctx context.Context, req *SetConsumerGroupOffsetRequest) (*SetConsumerGroupOffsetResponse, error) {
	// If no generation ID or member ID is provided, we need to create a temporary
	// consumer group session to get valid IDs
	if req.GenerationID == 0 || req.MemberID == "" {
		// Get the topics from the request to create a valid consumer group
		topics := make([]string, 0, len(req.Topics))
		for topicName := range req.Topics {
			topics = append(topics, topicName)
		}

		// Create a temporary consumer group to get valid generation and member IDs
		group, err := NewConsumerGroup(ConsumerGroupConfig{
			ID:                req.GroupID,
			Topics:            topics,
			Brokers:           []string{"localhost:9092"}, // Default broker
			HeartbeatInterval: 2 * time.Second,
			RebalanceTimeout:  2 * time.Second,
			RetentionTime:     time.Hour,
		})
		if err != nil {
			return nil, fmt.Errorf("kafka.(*Client).SetConsumerGroupOffset: failed to create temporary consumer group: %w", err)
		}
		defer group.Close()

		// Get a generation to obtain valid IDs
		gen, err := group.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("kafka.(*Client).SetConsumerGroupOffset: failed to get generation: %w", err)
		}

		// Use the obtained generation and member IDs
		req.GenerationID = int(gen.ID)
		req.MemberID = gen.MemberID
	}

	// Convert the request to use the existing OffsetCommit function
	offsetCommits := make(map[string][]OffsetCommit)
	for topicName, offsets := range req.Topics {
		commits := make([]OffsetCommit, len(offsets))
		for i, offset := range offsets {
			commits[i] = OffsetCommit{
				Partition: offset.Partition,
				Offset:    offset.Offset,
				Metadata:  offset.Metadata,
			}
		}
		offsetCommits[topicName] = commits
	}

	// Use the existing OffsetCommit function
	offsetCommitReq := &OffsetCommitRequest{
		Addr:         req.Addr,
		GroupID:      req.GroupID,
		GenerationID: req.GenerationID,
		MemberID:     req.MemberID,
		InstanceID:   req.InstanceID,
		Topics:       offsetCommits,
	}

	offsetCommitResp, err := c.OffsetCommit(ctx, offsetCommitReq)
	if err != nil {
		return nil, fmt.Errorf("kafka.(*Client).SetConsumerGroupOffset: %w", err)
	}

	// Convert the response back to our format
	res := &SetConsumerGroupOffsetResponse{
		Throttle: offsetCommitResp.Throttle,
		Topics:   make(map[string][]SetOffsetPartition, len(offsetCommitResp.Topics)),
	}

	for topicName, partitions := range offsetCommitResp.Topics {
		setOffsetPartitions := make([]SetOffsetPartition, len(partitions))
		for i, partition := range partitions {
			setOffsetPartitions[i] = SetOffsetPartition{
				Partition: partition.Partition,
				Error:     partition.Error,
			}
		}
		res.Topics[topicName] = setOffsetPartitions
	}

	return res, nil
}
