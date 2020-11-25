package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go/protocol/createpartitions"
)

type CreatePartitionsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	Topic         string
	NewPartitions []CreatePartitionsRequestPartition
	TotalCount    int
	Timeout       time.Duration
}

type CreatePartitionsRequestPartition struct {
	BrokerIDs []int
}

type CreatePartitionsResponse struct {
	ErrorCode    int
	ErrorMessage string
}

func (c *Client) CreatePartitions(
	ctx context.Context,
	req CreatePartitionsRequest,
) (*CreatePartitionsResponse, error) {
	assignments := []createpartitions.RequestAssignment{}
	for _, partition := range req.NewPartitions {
		brokerIDs32 := []int32{}
		for _, brokerID := range partition.BrokerIDs {
			brokerIDs32 = append(brokerIDs32, int32(brokerID))
		}

		assignments = append(assignments, createpartitions.RequestAssignment{
			BrokerIDs: brokerIDs32,
		})
	}

	apiReq := &createpartitions.Request{
		Topics: []createpartitions.RequestTopic{
			{
				Name:        req.Topic,
				Count:       int32(req.TotalCount),
				Assignments: assignments,
			},
		},
		TimeoutMs: int32(req.Timeout.Milliseconds()),
	}

	protoResp, err := c.roundTrip(
		ctx,
		req.Addr,
		apiReq,
	)
	if err != nil {
		return nil, err
	}
	apiResp := protoResp.(*createpartitions.Response)

	if len(apiResp.Results) == 0 {
		return nil, fmt.Errorf("Empty results")
	}

	return &CreatePartitionsResponse{
		ErrorCode:    int(apiResp.Results[0].ErrorCode),
		ErrorMessage: apiResp.Results[0].ErrorMessage,
	}, nil
}
