package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestTopicScanner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T, context.Context)
	}{
		{
			scenario: "calling GetTopics as new topics are being made will eventually return the new topics",
			function: testGetTopicsUpdated,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			testFunc(t, ctx)
		})
	}
}

func testGetTopicsUpdated(t *testing.T, ctx context.Context) {

	id := rand.Int63()
	ts, err := NewTopicScanner(TopicScannerConfig{
		TopicRegex:         fmt.Sprintf("kafka-go-ts-%016x-", id),
		Brokers:            []string{"localhost:9092"},
		ScanningIntervalMS: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()
	conn, err := DialContext(context.Background(), "tcp", "localhost:9092")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	err = conn.CreateTopics(TopicConfig{
		Topic:             fmt.Sprintf("kafka-go-ts-%016x-1", id),
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ts.StartScanning(context.Background())
	desiredTopicCount := 1
outer:
	for {
		select {
		case _ = <-ctx.Done():
			t.Fatal(fmt.Sprintf("Timed out waiting for Topic Scanner topics to reach desired amount: %d", desiredTopicCount))
		default:
			if len(ts.GetTopics()) == desiredTopicCount {
				break outer
			}
		}
	}

	err = conn.CreateTopics(TopicConfig{
		Topic:             fmt.Sprintf("kafka-go-ts-%016x-2", id),
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	desiredTopicCount = 2
outer2:
	for {
		select {
		case _ = <-ctx.Done():
			t.Fatal(fmt.Sprintf("Timed out waiting for Topic Scanner topics to reach desired amount: %d", desiredTopicCount))
		default:
			if len(ts.GetTopics()) == desiredTopicCount {
				break outer2
			}
		}
	}

}
