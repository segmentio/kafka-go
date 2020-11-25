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
	PartitionID int
	BrokerIDs   []int
}

type AlterPartitionReassignmentsResponse struct {
	ErrorCode    int
	ErrorMessage string

	PartitionResults []AlterPartitionReassignmentsResponsePartitionResult
}

type AlterPartitionReassignmentsResponsePartitionResult struct {
	PartitionID  int
	ErrorCode    int
	ErrorMessage string
}

func (c *Client) AlterPartitionReassignments(
	ctx context.Context,
	req AlterPartitionReassignmentsRequest,
) (*AlterPartitionReassignmentsResponse, error) {
	apiPartitions := []alterpartitionreassignments.RequestPartition{}

	for _, assignment := range req.Assignments {
		replicas := []int32{}
		for _, brokerID := range assignment.BrokerIDs {
			replicas = append(replicas, int32(brokerID))
		}

		apiPartitions = append(
			apiPartitions,
			alterpartitionreassignments.RequestPartition{
				PartitionIndex: int32(assignment.PartitionID),
				Replicas:       replicas,
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

	resp := &AlterPartitionReassignmentsResponse{
		ErrorCode:    int(apiResp.ErrorCode),
		ErrorMessage: apiResp.ErrorMessage,
	}

	for _, topicResult := range apiResp.Results {
		for _, partitionResult := range topicResult.Partitions {
			resp.PartitionResults = append(
				resp.PartitionResults,
				AlterPartitionReassignmentsResponsePartitionResult{
					PartitionID:  int(partitionResult.PartitionIndex),
					ErrorCode:    int(partitionResult.ErrorCode),
					ErrorMessage: partitionResult.ErrorMessage,
				},
			)
		}
	}

	return resp, nil
}
