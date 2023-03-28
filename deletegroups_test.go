package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestClientDeleteGroups(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("1.1.0") {
		t.Skip("Skipping test because kafka version is not high enough.")
	}

	client, shutdown := newLocalClient()
	defer shutdown()

	topic := makeTopic()
	createTopic(t, topic, 1)

	groupID := makeGroupID()

	group, err := NewConsumerGroup(ConsumerGroupConfig{
		ID:                groupID,
		Topics:            []string{topic},
		Brokers:           []string{"localhost:9092"},
		HeartbeatInterval: 2 * time.Second,
		RebalanceTimeout:  2 * time.Second,
		RetentionTime:     time.Hour,
		Logger:            &testKafkaLogger{T: t},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gen, err := group.Next(ctx)
	if gen == nil {
		t.Fatalf("expected generation 1 not to be nil")
	}
	if err != nil {
		t.Fatalf("expected no error, but got %+v", err)
	}

	// delete not empty group
	res, err := client.DeleteGroups(ctx, &DeleteGroupsRequest{
		GroupIDs: []string{groupID},
	})

	if err != nil {
		t.Fatal(err)
	}

	if !errors.Is(res.Errors[groupID], NonEmptyGroup) {
		t.Fatalf("expected NonEmptyGroup error, but got %+v", res.Errors[groupID])
	}

	err = group.Close()
	if err != nil {
		t.Fatal(err)
	}

	// delete empty group
	res, err = client.DeleteGroups(ctx, &DeleteGroupsRequest{
		GroupIDs: []string{groupID},
	})

	if err != nil {
		t.Fatal(err)
	}

	if err = res.Errors[groupID]; err != nil {
		t.Error(err)
	}
}
