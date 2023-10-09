package kafka

import (
	"context"
	"net"
	"time"

	"github.com/segmentio/kafka-go/protocol/alterpartitionreassignments"
)

// AlterPartitionReassignmentsRequest is a request to the AlterPartitionReassignments API.
type AlterPartitionReassignmentsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	// Mappings of topic names to list of partitions we want to reassign.
	Topics map[string][]AlterPartitionReassignmentsRequestAssignment

	// Timeout is the amount of time to wait for the request to complete.
	Timeout time.Duration
}

// AlterPartitionReassignmentsRequestAssignment contains the requested reassignments for a single
// partition.
type AlterPartitionReassignmentsRequestAssignment struct {
	// PartitionID is the ID of the partition to make the reassignments in.
	PartitionID int

	// BrokerIDs is a slice of brokers to set the partition replicas to, or null to cancel a pending reassignment for this partition.
	BrokerIDs []int
}

// AlterPartitionReassignmentsResponse is a response from the AlterPartitionReassignments API.
type AlterPartitionReassignmentsResponse struct {
	// Error is set to a non-nil value including the code and message if a top-level
	// error was encountered when doing the update.
	Error error

	// Mappings of topic names to list of reassignment results for each partition.
	Topics map[string][]AlterPartitionReassignmentsResponsePartitionResult
}

// AlterPartitionReassignmentsResponsePartitionResult contains the detailed result of
// doing reassignments for a single partition.
type AlterPartitionReassignmentsResponsePartitionResult struct {
	// PartitionID is the ID of the partition that was altered.
	PartitionID int

	// Error is set to a non-nil value including the code and message if an error was encountered
	// during the update for this partition.
	Error error
}

func (c *Client) AlterPartitionReassignments(
	ctx context.Context,
	req *AlterPartitionReassignmentsRequest,
) (*AlterPartitionReassignmentsResponse, error) {
	apiReq := &alterpartitionreassignments.Request{
		TimeoutMs: int32(req.Timeout.Milliseconds()),
	}

	for topicName, assignments := range req.Topics {
		apiPartitions := []alterpartitionreassignments.RequestPartition{}

		for _, assignment := range assignments {
			var replicas []int32
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
		apiReq.Topics = append(apiReq.Topics, alterpartitionreassignments.RequestTopic{
			Name:       topicName,
			Partitions: apiPartitions,
		})
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
		Error:  makeError(apiResp.ErrorCode, apiResp.ErrorMessage),
		Topics: make(map[string][]AlterPartitionReassignmentsResponsePartitionResult),
	}

	for _, topicResult := range apiResp.Results {
		respTopic := []AlterPartitionReassignmentsResponsePartitionResult{}
		for _, partitionResult := range topicResult.Partitions {
			respTopic = append(
				respTopic,
				AlterPartitionReassignmentsResponsePartitionResult{
					PartitionID: int(partitionResult.PartitionIndex),
					Error:       makeError(partitionResult.ErrorCode, partitionResult.ErrorMessage),
				},
			)
		}
		resp.Topics[topicResult.Name] = respTopic
	}

	return resp, nil
}
