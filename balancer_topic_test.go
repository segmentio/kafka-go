package kafka

import (
	"sync"
	"testing"
)

func TestRoundRobinTopicAwareInterleavedTopics(t *testing.T) {
	rr := &RoundRobin{}
	for i := 0; i < 100; i++ {
		a := rr.Balance(Message{Topic: "orders"}, 0, 1)
		b := rr.Balance(Message{Topic: "payments"}, 0, 1)
		if a != i%2 || b != i%2 {
			t.Fatalf("at message %d, orders=%d, payments=%d; each should independently advance through partitions", i, a, b)
		}
	}
}

func TestRoundRobinTopicAwareChunksPerTopic(t *testing.T) {
	rr := &RoundRobin{ChunkSize: 3}
	for i := 0; i < 30; i++ {
		for _, topic := range []string{"a", "b"} {
			actual := rr.Balance(Message{Topic: topic}, 0, 1, 2)
			expected := (i / 3) % 3
			if actual != expected {
				t.Fatalf("topic %q iteration %d: got %d, want %d", topic, i, actual, expected)
			}
		}
	}
}

func TestRoundRobinTopicAwareEmptyTopicLegacy(t *testing.T) {
	rr := &RoundRobin{ChunkSize: 2}
	for i := 0; i < 30; i++ {
		got := rr.Balance(Message{}, 0, 1, 2)
		want := (i / 2) % 3
		if got != want {
			t.Fatalf("iteration %d: got %d want %d", i, got, want)
		}
	}
}

func TestRoundRobinTopicAwareConcurrentPerTopic(t *testing.T) {
	rr := &RoundRobin{}
	var wg sync.WaitGroup
	type result struct {
		topic  string
		counts [3]int
	}
	totals := make(chan result, 16)
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			topic := "a"
			if worker%2 == 1 {
				topic = "b"
			}
			var counts [3]int
			for i := 0; i < 300; i++ {
				p := rr.Balance(Message{Topic: topic}, 0, 1, 2)
				counts[p]++
			}
			totals <- result{topic: topic, counts: counts}
		}(worker)
	}
	wg.Wait()
	close(totals)
	byTopic := map[string][3]int{}
	for result := range totals {
		counts := byTopic[result.topic]
		for p, n := range result.counts {
			counts[p] += n
		}
		byTopic[result.topic] = counts
	}
	for topic, counts := range byTopic {
		for partition, n := range counts {
			if n != 800 {
				t.Fatalf("topic %s partition %d: %d messages, expected 800", topic, partition, n)
			}
		}
	}
}

func TestRoundRobinTopicAwareDifferentPartitionCounts(t *testing.T) {
	rr := &RoundRobin{}
	for i := 0; i < 60; i++ {
		tests := []struct {
			topic      string
			partitions []int
		}{
			{"orders", []int{0, 1}},
			{"payments", []int{0, 1, 2}},
			{"shipments", []int{0, 1, 2, 3, 4}},
		}
		for _, tc := range tests {
			actual := rr.Balance(Message{Topic: tc.topic}, tc.partitions...)
			expected := tc.partitions[i%len(tc.partitions)]
			if actual != expected {
				t.Fatalf("topic %s iteration %d: got %d, want %d", tc.topic, i, actual, expected)
			}
		}
	}
}

func TestRoundRobinTopicAwareNamedAndUnnamedIndependent(t *testing.T) {
	rr := &RoundRobin{}
	for i := 0; i < 20; i++ {
		if got := rr.Balance(Message{Topic: "explicit"}, 0, 1); got != i%2 {
			t.Fatalf("named topic iteration %d got %d", i, got)
		}
		if got := rr.Balance(Message{}, 0, 1); got != i%2 {
			t.Fatalf("unnamed topic iteration %d got %d", i, got)
		}
	}
}
