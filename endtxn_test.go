package kafka

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol/endtxn"
)

type endTxnMockTransport struct {
	response Response
	err      error
	captured *endtxn.Request
}

func (m *endTxnMockTransport) RoundTrip(ctx context.Context, addr net.Addr, req Request) (Response, error) {
	if m.err != nil {
		return nil, m.err
	}
	if r, ok := req.(*endtxn.Request); ok {
		m.captured = r
	}
	return m.response, nil
}

func TestClientEndTxn_Legacy(t *testing.T) {
	mockResp := &endtxn.Response{
		ThrottleTimeMs: 25,
		ErrorCode:      0,
	}

	transport := &endTxnMockTransport{response: mockResp}
	client := &Client{Transport: transport}

	resp, err := client.EndTxn(context.Background(), &EndTxnRequest{
		Addr:            TCP("localhost:9092"),
		TransactionalID: "test-tx",
		ProducerID:      1001,
		ProducerEpoch:   5,
		Committed:       true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Throttle != 25*time.Millisecond {
		t.Errorf("expected throttle 25ms, got %v", resp.Throttle)
	}
	if resp.Error != nil {
		t.Errorf("expected nil error, got %v", resp.Error)
	}
	if transport.captured == nil {
		t.Fatal("expected request to be captured")
	}
	if transport.captured.TransactionalID != "test-tx" {
		t.Errorf("expected transactional ID test-tx, got %s", transport.captured.TransactionalID)
	}
	if transport.captured.ProducerID != 1001 {
		t.Errorf("expected producer ID 1001, got %d", transport.captured.ProducerID)
	}
	if transport.captured.ProducerEpoch != 5 {
		t.Errorf("expected producer epoch 5, got %d", transport.captured.ProducerEpoch)
	}
	if !transport.captured.Committed {
		t.Errorf("expected committed true")
	}
}

func TestClientEndTxn_TransactionsV2(t *testing.T) {
	// Under KIP-890 (Transactions v2 / Kafka 4.0+), EndTxnResponse v5 returns the bumped ProducerID and ProducerEpoch.
	mockResp := &endtxn.Response{
		ThrottleTimeMs: 15,
		ErrorCode:      0,
		ProducerID:     1001,
		ProducerEpoch:  6,
	}

	transport := &endTxnMockTransport{response: mockResp}
	client := &Client{Transport: transport}

	resp, err := client.EndTxn(context.Background(), &EndTxnRequest{
		Addr:            TCP("localhost:9092"),
		TransactionalID: "test-tx",
		ProducerID:      1001,
		ProducerEpoch:   5,
		Committed:       true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ProducerID != 1001 {
		t.Errorf("expected bumped ProducerID 1001, got %d", resp.ProducerID)
	}
	if resp.ProducerEpoch != 6 {
		t.Errorf("expected bumped ProducerEpoch 6, got %d", resp.ProducerEpoch)
	}
}

func TestClientEndTxn_TransactionAbortableError(t *testing.T) {
	mockResp := &endtxn.Response{
		ErrorCode: int16(TransactionAbortable),
	}

	transport := &endTxnMockTransport{response: mockResp}
	client := &Client{Transport: transport}

	resp, err := client.EndTxn(context.Background(), &EndTxnRequest{
		Addr:            TCP("localhost:9092"),
		TransactionalID: "test-tx",
		ProducerID:      1001,
		ProducerEpoch:   5,
		Committed:       true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !errors.Is(resp.Error, TransactionAbortable) {
		t.Fatalf("expected TransactionAbortable error, got %v", resp.Error)
	}
}
