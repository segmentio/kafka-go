package kafka

import (
	"context"
	"testing"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestClientAlterPartitionReassignments(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("2.4.0") {
		return
	}

	ctx := context.Background()
	client, shutdown := newLocalClient()
	defer shutdown()

	topic1 := makeTopic()
	topic2 := makeTopic()
	createTopic(t, topic1, 2)
	createTopic(t, topic2, 2)
	defer func() {
		deleteTopic(t, topic1)
		deleteTopic(t, topic2)
	}()

	// Local kafka only has 1 broker, so any partition reassignments are really no-ops.
	resp, err := client.AlterPartitionReassignments(
		ctx,
		&AlterPartitionReassignmentsRequest{
			Topics: map[string][]AlterPartitionReassignmentsRequestAssignment{
				topic1: {
					{
						PartitionID: 0,
						BrokerIDs:   []int{1},
					},
					{
						PartitionID: 1,
						BrokerIDs:   []int{1},
					},
				},
				topic2: {
					{
						PartitionID: 0,
					},
				},
			},
		},
	)

	if err != nil {
		t.Fatal(err)
	}
	if resp.Error != nil {
		t.Error(
			"Unexpected error in response",
			"expected", nil,
			"got", resp.Error,
		)
	}
	if len(resp.Topics) != 2 {
		t.Error(
			"Unexpected length of topic results",
			"expected", 2,
			"got", len(resp.Topics),
		)
	}
	if len(resp.Topics[topic1]) != 2 {
		t.Error(
			"Unexpected length of partition results",
			"expected", 2,
			"got", len(resp.Topics[topic1]),
		)
	}
	if len(resp.Topics[topic2]) != 1 {
		t.Error(
			"Unexpected length of partition results",
			"expected", 1,
			"got", len(resp.Topics[topic2]),
		)
	}
}
