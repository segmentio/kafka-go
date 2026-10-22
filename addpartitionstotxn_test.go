package kafka

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol/addpartitionstotxn"
	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestClientAddPartitionsToTxn(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("0.11.0") {
		t.Skip("Skipping test because kafka version is not high enough.")
	}

	// TODO: look into why this test fails on Kafka 3.0.0 and higher when transactional support
	// work is revisited.
	if ktesting.KafkaIsAtLeast("3.0.0") {
		t.Skip("Skipping test because it fails on Kafka version 3.0.0 or higher.")
	}

	topic1 := makeTopic()
	topic2 := makeTopic()

	client, shutdown := newLocalClient()
	defer shutdown()

	err := clientCreateTopic(client, topic1, 3)
	if err != nil {
		t.Fatal(err)
	}

	err = clientCreateTopic(client, topic2, 3)
	if err != nil {
		t.Fatal(err)
	}

	transactionalID := makeTransactionalID()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	respc, err := waitForCoordinatorIndefinitely(ctx, client, &FindCoordinatorRequest{
		Addr:    client.Addr,
		Key:     transactionalID,
		KeyType: CoordinatorKeyTypeTransaction,
	})
	if err != nil {
		t.Fatal(err)
	}

	transactionCoordinator := TCP(net.JoinHostPort(respc.Coordinator.Host, strconv.Itoa(int(respc.Coordinator.Port))))
	client, shutdown = newClient(transactionCoordinator)
	defer shutdown()

	ipResp, err := client.InitProducerID(ctx, &InitProducerIDRequest{
		TransactionalID:      transactionalID,
		TransactionTimeoutMs: 10000,
	})
	if err != nil {
		t.Fatal(err)
	}

	if ipResp.Error != nil {
		t.Fatal(ipResp.Error)
	}

	defer func() {
		err := clientEndTxn(client, &EndTxnRequest{
			TransactionalID: transactionalID,
			ProducerID:      ipResp.Producer.ProducerID,
			ProducerEpoch:   ipResp.Producer.ProducerEpoch,
			Committed:       false,
		})
		if err != nil {
			t.Fatal(err)
		}
	}()

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	resp, err := client.AddPartitionsToTxn(ctx, &AddPartitionsToTxnRequest{
		TransactionalID: transactionalID,
		ProducerID:      ipResp.Producer.ProducerID,
		ProducerEpoch:   ipResp.Producer.ProducerEpoch,
		Topics: map[string][]AddPartitionToTxn{
			topic1: {
				{
					Partition: 0,
				},
				{
					Partition: 1,
				},
				{
					Partition: 2,
				},
			},
			topic2: {
				{
					Partition: 0,
				},
				{
					Partition: 2,
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(resp.Topics) != 2 {
		t.Errorf("expected responses for 2 topics; got: %d", len(resp.Topics))
	}
	for topic, partitions := range resp.Topics {
		if topic == topic1 {
			if len(partitions) != 3 {
				t.Errorf("expected 3 partitions in response for topic %s; got: %d", topic, len(partitions))
			}
		}
		if topic == topic2 {
			if len(partitions) != 2 {
				t.Errorf("expected 2 partitions in response for topic %s; got: %d", topic, len(partitions))
			}
		}
		for _, partition := range partitions {
			if partition.Error != nil {
				t.Error(partition.Error)
			}
		}
	}
}

type addPartitionsMockTransport struct {
	response Response
	err      error
	captured *addpartitionstotxn.Request
}

func (m *addPartitionsMockTransport) RoundTrip(ctx context.Context, addr net.Addr, req Request) (Response, error) {
	if m.err != nil {
		return nil, m.err
	}
	if r, ok := req.(*addpartitionstotxn.Request); ok {
		m.captured = r
	}
	return m.response, nil
}

func TestAddPartitionsToTxn_LegacyResults(t *testing.T) {
	mockResp := &addpartitionstotxn.Response{
		ThrottleTimeMs: 50,
		Results: []addpartitionstotxn.ResponseResult{
			{
				Name: "topic-1",
				Results: []addpartitionstotxn.ResponsePartition{
					{PartitionIndex: 0, ErrorCode: 0},
					{PartitionIndex: 1, ErrorCode: int16(ConcurrentTransactions)},
				},
			},
		},
	}

	transport := &addPartitionsMockTransport{response: mockResp}
	client := &Client{Transport: transport}

	res, err := client.AddPartitionsToTxn(context.Background(), &AddPartitionsToTxnRequest{
		Addr:            TCP("localhost:9092"),
		TransactionalID: "tx-1",
		ProducerID:      100,
		ProducerEpoch:   1,
		Topics: map[string][]AddPartitionToTxn{
			"topic-1": {{Partition: 0}, {Partition: 1}},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Throttle != 50*time.Millisecond {
		t.Errorf("expected throttle 50ms, got %v", res.Throttle)
	}
	if res.Error != nil {
		t.Errorf("expected nil top-level error, got %v", res.Error)
	}
	if len(res.Topics["topic-1"]) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(res.Topics["topic-1"]))
	}
	if !errors.Is(res.Topics["topic-1"][1].Error, ConcurrentTransactions) {
		t.Errorf("expected ConcurrentTransactions error, got %v", res.Topics["topic-1"][1].Error)
	}
}

func TestAddPartitionsToTxn_KIP890_ResultsByTransaction(t *testing.T) {
	mockResp := &addpartitionstotxn.Response{
		ThrottleTimeMs: 10,
		ErrorCode:      int16(TransactionAbortable),
		ResultsByTransaction: []addpartitionstotxn.ResponseTransactionResult{
			{
				TransactionalID: "tx-2",
				TopicResults: []addpartitionstotxn.ResponseResult{
					{
						Name: "topic-kip890",
						Results: []addpartitionstotxn.ResponsePartition{
							{PartitionIndex: 0, ErrorCode: 0},
							{PartitionIndex: 1, ErrorCode: int16(TransactionAbortable)},
						},
					},
				},
			},
		},
	}

	transport := &addPartitionsMockTransport{response: mockResp}
	client := &Client{Transport: transport}

	res, err := client.AddPartitionsToTxn(context.Background(), &AddPartitionsToTxnRequest{
		Addr:            TCP("localhost:9092"),
		TransactionalID: "tx-2",
		ProducerID:      200,
		ProducerEpoch:   2,
		VerifyOnly:      true,
		Topics: map[string][]AddPartitionToTxn{
			"topic-kip890": {{Partition: 0}, {Partition: 1}},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if transport.captured == nil {
		t.Fatal("expected request to be captured")
	}
	if !transport.captured.VerifyOnly {
		t.Errorf("expected VerifyOnly to be true on protoReq")
	}
	if !errors.Is(res.Error, TransactionAbortable) {
		t.Errorf("expected top-level TransactionAbortable, got %v", res.Error)
	}
	parts := res.Topics["topic-kip890"]
	if len(parts) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(parts))
	}
	if parts[0].Error != nil {
		t.Errorf("expected partition 0 to have nil error, got %v", parts[0].Error)
	}
	if !errors.Is(parts[1].Error, TransactionAbortable) {
		t.Errorf("expected partition 1 to have TransactionAbortable error, got %v", parts[1].Error)
	}
}
