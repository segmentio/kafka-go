package kafka

import (
	"context"
	"testing"
	"time"
)

func TestClientSetConsumerGroupOffset(t *testing.T) {
	topic := makeTopic()
	client, shutdown := newLocalClientWithTopic(topic, 3)
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	groupID := makeGroupID()

	// First, let's create a consumer group to ensure the group exists
	group, err := NewConsumerGroup(ConsumerGroupConfig{
		ID:                groupID,
		Topics:            []string{topic},
		Brokers:           []string{"localhost:9092"},
		HeartbeatInterval: 2 * time.Second,
		RebalanceTimeout:  2 * time.Second,
		RetentionTime:     time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()

	// Get a generation to ensure the group is properly initialized
	gen, err := group.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Now test setting offsets explicitly
	resp, err := client.SetConsumerGroupOffset(ctx, &SetConsumerGroupOffsetRequest{
		GroupID:      groupID,
		GenerationID: int(gen.ID),
		MemberID:     gen.MemberID,
		Topics: map[string][]SetOffset{
			topic: {
				{Partition: 0, Offset: 10, Metadata: "test-metadata"},
				{Partition: 1, Offset: 15, Metadata: "test-metadata"},
				{Partition: 2, Offset: 20, Metadata: "test-metadata"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify the response
	if resp.Throttle < 0 {
		t.Errorf("expected non-negative throttle time, got %v", resp.Throttle)
	}

	topicResponses, exists := resp.Topics[topic]
	if !exists {
		t.Fatalf("expected response for topic %s", topic)
	}

	if len(topicResponses) != 3 {
		t.Fatalf("expected 3 partition responses, got %d", len(topicResponses))
	}

	// Check that we have responses for all expected partitions
	expectedPartitions := map[int]bool{0: true, 1: true, 2: true}
	for _, partitionResp := range topicResponses {
		if !expectedPartitions[partitionResp.Partition] {
			t.Errorf("unexpected partition %d in response", partitionResp.Partition)
		}
		if partitionResp.Error != nil {
			t.Errorf("partition %d had error: %v", partitionResp.Partition, partitionResp.Error)
		}
	}
}

func TestClientSetConsumerGroupOffsetWithoutGeneration(t *testing.T) {
	topic := makeTopic()
	client, shutdown := newLocalClientWithTopic(topic, 3)
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	groupID := makeGroupID()

	// Test setting offsets without being part of an active generation
	// This should work now because the function creates a temporary session
	resp, err := client.SetConsumerGroupOffset(ctx, &SetConsumerGroupOffsetRequest{
		GroupID:      groupID,
		GenerationID: 0,  // Not part of an active generation
		MemberID:     "", // No member ID
		Topics: map[string][]SetOffset{
			topic: {
				{Partition: 0, Offset: 5, Metadata: "standalone-metadata"},
				{Partition: 1, Offset: 8, Metadata: "standalone-metadata"},
				{Partition: 2, Offset: 12, Metadata: "standalone-metadata"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify the response
	topicResponses, exists := resp.Topics[topic]
	if !exists {
		t.Fatalf("expected response for topic %s", topic)
	}

	if len(topicResponses) != 3 {
		t.Fatalf("expected 3 partition responses, got %d", len(topicResponses))
	}

	// Check that we have responses for all expected partitions
	expectedPartitions := map[int]bool{0: true, 1: true, 2: true}
	for _, partitionResp := range topicResponses {
		if !expectedPartitions[partitionResp.Partition] {
			t.Errorf("unexpected partition %d in response", partitionResp.Partition)
		}
		if partitionResp.Error != nil {
			t.Errorf("partition %d had error: %v", partitionResp.Partition, partitionResp.Error)
		}
	}
}

func TestClientSetConsumerGroupOffsetInvalidGroup(t *testing.T) {
	topic := makeTopic()
	client, shutdown := newLocalClientWithTopic(topic, 3)
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	// Test with a non-existent group ID
	resp, err := client.SetConsumerGroupOffset(ctx, &SetConsumerGroupOffsetRequest{
		GroupID:      "non-existent-group",
		GenerationID: 0,
		MemberID:     "",
		Topics: map[string][]SetOffset{
			topic: {
				{Partition: 0, Offset: 5, Metadata: "test"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// The request should succeed even for non-existent groups
	// as Kafka will create the group if it doesn't exist
	topicResponses, exists := resp.Topics[topic]
	if !exists {
		t.Fatalf("expected response for topic %s", topic)
	}

	if len(topicResponses) != 1 {
		t.Fatalf("expected 1 partition response, got %d", len(topicResponses))
	}

	if topicResponses[0].Error != nil {
		t.Errorf("unexpected error: %v", topicResponses[0].Error)
	}
}
