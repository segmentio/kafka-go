package kafka

import (
	"context"
	"testing"
	"time"
)

func TestClusterClient(t *testing.T) {
	t.Parallel()
	tests := []struct {
		scenario string
		function func(*testing.T, context.Context, *ClusterClient)
	}{
		{
			scenario: "calling Read with a context that has been canceled returns an error",
			function: testConsumerGroupFetchOffsets,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			cc := NewClusterClient([]string{"localhost:9092"})
			//defer cc.Close()
			testFunc(t, ctx, cc)
		})
	}
}

func testConsumerGroupFetchOffsets(t *testing.T, ctx context.Context, cc *ClusterClient) {
	const N = 144

	topic := makeTopic()
	groupId := makeGroupID()
	createTopic(t, topic, 12)

	writer := NewWriter(WriterConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     topic,
		Dialer:    DefaultDialer,
		Balancer:  &RoundRobin{},
		BatchSize: 1,
	})
	if err := writer.WriteMessages(ctx, makeTestSequence(N)...); err != nil {
		t.Fatalf("bad write messages: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("bad write err: %v", err)
	}

	r := NewReader(ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic,
		GroupID:  groupId,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  100 * time.Millisecond,
	})
	defer r.Close()

	partitions := map[int]struct{}{}
	for i := 0; i < N; i++ {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			t.Errorf("bad error: %s", err)
		}
		partitions[m.Partition] = struct{}{}
		r.CommitMessages(context.Background(), m)
	}

	if v := len(partitions); v != 12 {
		t.Errorf("expected messages across 12 partitions; got messages across %v partitions", v)
	}
}
