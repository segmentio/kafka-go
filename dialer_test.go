package kafka

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestDialer(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T, context.Context, *Dialer)
	}{
		{
			scenario: "looking up partitions returns the list of available partitions for a topic",
			function: testDialerLookupPartitions,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			testFunc(t, ctx, &Dialer{})
		})
	}
}

func testDialerLookupPartitions(t *testing.T, ctx context.Context, d *Dialer) {
	// Write a message to ensure the partition gets created.
	w := NewWriter(WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-dialer-LookupPartitions",
	})
	w.WriteMessages(ctx, Message{})
	w.Close()

	// for some reason the partition isn't available right away.
	time.Sleep(1 * time.Second)

	partitions, err := d.LookupPartitions(ctx, "tcp", "localhost:9092", "test-dialer-LookupPartitions")

	if err != nil {
		t.Error(err)
		return
	}

	sort.Slice(partitions, func(i int, j int) bool {
		return partitions[i].ID < partitions[j].ID
	})

	if !reflect.DeepEqual(partitions, []Partition{
		{
			Topic:    "test-dialer-LookupPartitions",
			Leader:   Broker{Host: "localhost", Port: 9092, ID: 1001},
			Replicas: []Broker{{Host: "localhost", Port: 9092, ID: 1001}},
			Isr:      []Broker{{Host: "localhost", Port: 9092, ID: 1001}},
			ID:       0,
		},
	}) {
		t.Error("bad partitions:", partitions)
	}
}
