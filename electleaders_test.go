package kafka

import (
	"context"
	"testing"
	"time"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestClientElectLeaders(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("2.4.0") {
		return
	}

	ctx := context.Background()
	client, shutdown := newLocalClient()
	// on Kraft mode leader elections take longer
	client.Timeout = 10 * time.Second
	defer shutdown()

	topic := makeTopic()
	createTopic(t, topic, 2)
	defer deleteTopic(t, topic)

	// Local kafka only has 1 broker, so leader elections are no-ops.
	resp, err := client.ElectLeaders(
		ctx,
		&ElectLeadersRequest{
			Topic:      topic,
			Partitions: []int{0, 1},
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
	if len(resp.PartitionResults) != 2 {
		t.Error(
			"Unexpected length of partition results",
			"expected", 2,
			"got", len(resp.PartitionResults),
		)
	}
}
