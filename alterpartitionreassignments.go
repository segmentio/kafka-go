package kafka

import (
	"context"
	"net"
	"time"

	"github.com/segmentio/kafka-go/protocol/alterpartitionreassignments"
)

type AlterPartitionReassignmentsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	Topic       string
	Assignments []AlterPartitionReassignmentsRequestAssignment
	Timeout     time.Duration
}

type AlterPartitionReassignmentsRequestAssignment struct {
	PartitionID int32
	BrokerIDs   []int32
}

type AlterPartitionReassignmentsResponse struct {
	ErrorCode    int16
	ErrorMessage string
}

func (c *Client) AlterPartitionReassignments(
	ctx context.Context,
	req AlterPartitionReassignmentsRequest,
) (*AlterPartitionReassignmentsResponse, error) {
	apiPartitions := []alterpartitionreassignments.RequestPartition{}

	for _, assignment := range req.Assignments {
		apiPartitions = append(
			apiPartitions,
			alterpartitionreassignments.RequestPartition{
				PartitionIndex: assignment.PartitionID,
				Replicas:       assignment.BrokerIDs,
			},
		)
	}

	apiReq := &alterpartitionreassignments.Request{
		TimeoutMs: int32(req.Timeout.Milliseconds()),
		Topics: []alterpartitionreassignments.RequestTopic{
			{
				Name:       req.Topic,
				Partitions: apiPartitions,
			},
		},
	}

	protoResp, err := c.roundTrip(
		ctx,
		req.Addr,
		apiReq,
	)
	if err != nil {
		return nil, err
	}
	apiResp := protoResp.(*alterpartitionreassignments.Response)

	return &AlterPartitionReassignmentsResponse{
		ErrorCode:    apiResp.ErrorCode,
		ErrorMessage: apiResp.ErrorMessage,
	}, nil
}
