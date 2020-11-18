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
	ErrorCode        int16
	PartitionResults []ElectLeadersResponsePartitionResult
}

type ElectLeadersResponsePartitionResult struct {
	Partition    int32
	ErrorCode    int16
	ErrorMessage string
}

func (c *Client) ElectLeaders(
	ctx context.Context,
	req ElectLeadersRequest,
) (*ElectLeadersResponse, error) {
	protoResp, err := c.roundTrip(
		ctx,
		req.Addr,
		&electleaders.Request{
			TopicPartitions: []electleaders.RequestTopicPartitions{
				{
					Topic:        req.Topic,
					PartitionIDs: req.Partitions,
				},
			},
			TimeoutMs: int32(req.Timeout.Milliseconds()),
		},
	)
	if err != nil {
		return nil, err
	}
	apiResp := protoResp.(*electleaders.Response)

	resp := &ElectLeadersResponse{
		ErrorCode: apiResp.ErrorCode,
	}

	for _, topicResult := range apiResp.ReplicaElectionResults {
		for _, partitionResult := range topicResult.PartitionResults {
			resp.PartitionResults = append(
				resp.PartitionResults,
				ElectLeadersResponsePartitionResult{
					Partition:    partitionResult.PartitionID,
					ErrorCode:    partitionResult.ErrorCode,
					ErrorMessage: partitionResult.ErrorMessage,
				},
			)
		}
	}

	return resp, nil
}
