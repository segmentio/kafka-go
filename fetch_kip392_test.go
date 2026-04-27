package kafka

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	fetchAPI "github.com/segmentio/kafka-go/protocol/fetch"
)

// stubRoundTripper returns a canned response for any request it receives.
type stubRoundTripper struct {
	resp protocol.Message
	err  error
}

func (s *stubRoundTripper) RoundTrip(_ context.Context, _ net.Addr, _ protocol.Message) (protocol.Message, error) {
	return s.resp, s.err
}

// TestClientFetch_PreferredReadReplicaDefaultsMinusOneWithoutRackID asserts
// that Client.Fetch surfaces PreferredReadReplica = -1 when the caller did
// not opt into KIP-392 (req.RackID == ""). Without this guard, a v10 broker
// response (or any response that omits the v11 field) would surface the Go
// zero value 0, indistinguishable from broker id 0 being preferred.
func TestClientFetch_PreferredReadReplicaDefaultsMinusOneWithoutRackID(t *testing.T) {
	stub := &stubRoundTripper{
		resp: &fetchAPI.Response{
			ThrottleTimeMs: 0,
			Topics: []fetchAPI.ResponseTopic{{
				Topic: "topic-x",
				Partitions: []fetchAPI.ResponsePartition{{
					Partition:            0,
					ErrorCode:            0,
					HighWatermark:        100,
					LastStableOffset:     100,
					LogStartOffset:       0,
					PreferredReadReplica: 0, // simulates v10 zero-value field
				}},
			}},
		},
	}

	client := &Client{
		Addr:      TCP("127.0.0.1:9092"),
		Timeout:   time.Second,
		Transport: stub,
	}

	resp, err := client.Fetch(context.Background(), &FetchRequest{
		Topic:     "topic-x",
		Partition: 0,
		Offset:    0,
		MinBytes:  1,
		MaxBytes:  1024,
		MaxWait:   100 * time.Millisecond,
		// RackID intentionally left empty -- caller did not opt in.
	})
	if err != nil {
		t.Fatalf("Client.Fetch: %v", err)
	}
	if resp.PreferredReadReplica != -1 {
		t.Fatalf("PreferredReadReplica: got %d, want -1 when RackID is empty", resp.PreferredReadReplica)
	}
}

// TestClientFetch_PreferredReadReplicaPassThroughWithRackID asserts that when
// the caller did opt into KIP-392 by setting RackID, broker id 0 is passed
// through verbatim (broker 0 may legitimately be the preferred replica).
func TestClientFetch_PreferredReadReplicaPassThroughWithRackID(t *testing.T) {
	stub := &stubRoundTripper{
		resp: &fetchAPI.Response{
			Topics: []fetchAPI.ResponseTopic{{
				Topic: "topic-x",
				Partitions: []fetchAPI.ResponsePartition{{
					Partition:            0,
					HighWatermark:        100,
					PreferredReadReplica: 7,
				}},
			}},
		},
	}

	client := &Client{
		Addr:      TCP("127.0.0.1:9092"),
		Timeout:   time.Second,
		Transport: stub,
	}

	resp, err := client.Fetch(context.Background(), &FetchRequest{
		Topic:     "topic-x",
		Partition: 0,
		Offset:    0,
		MinBytes:  1,
		MaxBytes:  1024,
		MaxWait:   100 * time.Millisecond,
		RackID:    "rack-nl",
	})
	if err != nil {
		t.Fatalf("Client.Fetch: %v", err)
	}
	if resp.PreferredReadReplica != 7 {
		t.Fatalf("PreferredReadReplica: got %d, want 7 (passed through verbatim)", resp.PreferredReadReplica)
	}
}
