package kafka

import (
	"context"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	t.Parallel()
	tests := []struct {
		scenario string
		function func(*testing.T, context.Context, *Client)
	}{
		{
			scenario: "retrieve committed offsets for a consumer group and topic",
			function: testConsumerGroupFetchOffsets,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			c := NewClient("localhost:9092")
			testFunc(t, ctx, c)
		})
	}
}

func testConsumerGroupFetchOffsets(t *testing.T, ctx context.Context, c *Client) {
	const totalMessages = 144
	const partitions = 12
	const msgPerPartition = totalMessages / partitions
	topic := makeTopic()
	groupId := makeGroupID()
	createTopic(t, topic, partitions)
	brokers := []string{"localhost:9092"}

	writer := NewWriter(WriterConfig{
		Brokers:   brokers,
		Topic:     topic,
		Dialer:    DefaultDialer,
		Balancer:  &RoundRobin{},
		BatchSize: 1,
	})
	if err := writer.WriteMessages(ctx, makeTestSequence(totalMessages)...); err != nil {
		t.Fatalf("bad write messages: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("bad write err: %v", err)
	}

	r := NewReader(ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupId,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  100 * time.Millisecond,
	})
	defer r.Close()

	for i := 0; i < totalMessages; i++ {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			t.Errorf("error fetching message: %s", err)
		}
		r.CommitMessages(context.Background(), m)
	}

	offsets, err := c.ConsumerOffsets(ctx, TopicAndGroup{GroupId: groupId, Topic: topic})
	if err != nil {
		t.Fatal(err)
	}

	if len(offsets) != partitions {
		t.Fatalf("expected %d partitions but only received offsets for %d", partitions, len(offsets))
	}

	for i := 0; i < partitions; i++ {
		committedOffset := offsets[i]
		if committedOffset != msgPerPartition {
			t.Fatalf("expected committed offset of %d but received %d", msgPerPartition, committedOffset)
		}
	}
}
