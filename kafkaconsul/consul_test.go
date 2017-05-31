package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestConsul(t *testing.T) {
	tests := []struct {
		scenario string
		test     func(*testing.T, context.Context, GroupConfig)
	}{
		{
			"should acquire a lock",
			acquireLock,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			config := GroupConfig{
				Name:       uuid.New().String(),
				Addr:       "http://localhost:8500",
				Brokers:    []string{"localhost:9092"},
				Topic:      "test",
				Partitions: 1,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			test.test(t, ctx, config)
		})
	}
}

func acquireLock(t *testing.T, ctx context.Context, config GroupConfig) {
	reader, err := NewGroupReader(ctx, config)
	if err != nil {
		t.Fatalf("failed to acquire a lock on the reader: %s", err.Error())
	}

	reader.Close()
}
