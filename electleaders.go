package kafka

import (
	"context"
	"net"
	"time"

	"github.com/segmentio/kafka-go/protocol/electleaders"
)

type ElectLeadersRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	Topic      string
	Partitions []int32

	Timeout time.Duration
}

type ElectLeadersResponse struct {
	ErrorCode int16
}

func (c *Client) ElectLeaders(
	ctx context.Context,
	req ElectLeadersRequest,
) (*ElectLeadersResponse, error) {
	topicPartitions := []electleaders.RequestTopicPartition{}

	for _, partition := range req.Partitions {
		topicPartitions = append(
			topicPartitions,
			electleaders.RequestTopicPartition{
				Topic:       req.Topic,
				PartitionID: partition,
			},
		)
	}

	protoResp, err := c.roundTrip(
		ctx,
		req.Addr,
		&electleaders.Request{
			TopicPartitions: topicPartitions,
			TimeoutMs:       int32(req.Timeout.Milliseconds()),
		},
	)
	if err != nil {
		return nil, err
	}
	apiResp := protoResp.(*electleaders.Response)

	return &ElectLeadersResponse{
		ErrorCode: apiResp.ErrorCode,
	}, nil
}
