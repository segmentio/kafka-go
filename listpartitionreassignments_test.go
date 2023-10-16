package kafka

import (
	"context"
	"testing"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestClientListPartitionReassignments(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("2.4.0") {
		return
	}

	ctx := context.Background()
	client, shutdown := newLocalClient()
	defer shutdown()

	topic := makeTopic()
	createTopic(t, topic, 2)
	defer deleteTopic(t, topic)

	// Can't really get an ongoing partition reassignment with local Kafka, so just do a superficial test here.
	resp, err := client.ListPartitionReassignments(
		ctx,
		&ListPartitionReassignmentsRequest{
			Topics: map[string]ListPartitionReassignmentsRequestTopic{
				topic: {PartitionIndexes: []int{0, 1}},
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
	if len(resp.Topics) != 0 {
		t.Error(
			"Unexpected length of topic results",
			"expected", 0,
			"got", len(resp.Topics),
		)
	}
}
