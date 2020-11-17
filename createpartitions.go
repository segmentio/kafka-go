package kafka

import (
	"context"
	"fmt"
	"net"

	"github.com/segmentio/kafka-go/protocol/createpartitions"
)

type CreatePartitionsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	Topic         string
	NewPartitions []CreatePartitionsRequestPartition
	TotalCount    int32
}

type CreatePartitionsRequestPartition struct {
	BrokerIDs []int32
}

type CreatePartitionsResponse struct {
	ErrorCode    int16
	ErrorMessage string
}

func (c *Client) CreatePartitions(
	ctx context.Context,
	req CreatePartitionsRequest,
) (*CreatePartitionsResponse, error) {
	assignments := []createpartitions.RequestAssignment{}
	for _, partition := range req.NewPartitions {
		assignments = append(assignments, createpartitions.RequestAssignment{
			BrokerIDs: partition.BrokerIDs,
		})
	}

	apiReq := &createpartitions.Request{
		Topics: []createpartitions.RequestTopic{
			{
				Name:        req.Topic,
				Count:       req.TotalCount,
				Assignments: assignments,
			},
		},
		TimeoutMs: 5000,
	}

	protocolResp, err := c.roundTrip(
		ctx,
		req.Addr,
		apiReq,
	)
	if err != nil {
		return nil, err
	}
	apiResp := protocolResp.(*createpartitions.Response)

	fmt.Printf("Response: %+v", *apiResp)
	if len(apiResp.Results) == 0 {
		return nil, fmt.Errorf("Empty results")
	}

	return &CreatePartitionsResponse{
		ErrorCode:    apiResp.Results[0].ErrorCode,
		ErrorMessage: apiResp.Results[0].ErrorMessage,
	}, nil
}