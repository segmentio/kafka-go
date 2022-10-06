package kafka

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestClientInitProducerId(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("0.11.0") {
		return
	}
	client, shutdown := newLocalClient()
	defer shutdown()

	tid := makeTransactionalID()
	// Wait for kafka setup and Coordinator to be available.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	respc, err := waitForCoordinatorIndefinitely(ctx, client, &FindCoordinatorRequest{
		Addr:    client.Addr,
		Key:     tid,
		KeyType: CoordinatorKeyTypeTransaction,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Now establish a connection with the transaction coordinator
	transactionCoordinator := TCP(net.JoinHostPort(respc.Coordinator.Host, strconv.Itoa(int(respc.Coordinator.Port))))
	client, shutdown = newClient(transactionCoordinator)
	defer shutdown()

	// Check if producer epoch increases and PID remains the same when producer is
	// initialized again with the same transactionalID
	resp, err := client.InitProducerID(context.Background(), &InitProducerIDRequest{
		Addr:                 transactionCoordinator,
		TransactionalID:      tid,
		TransactionTimeoutMs: 30000,
	})
	if err != nil {
		t.Fatal(err)
	}

	if resp.Error != nil {
		t.Fatal(resp.Error)
	}

	epoch1 := resp.Producer.ProducerEpoch
	pid1 := resp.Producer.ProducerID

	resp, err = client.InitProducerID(context.Background(), &InitProducerIDRequest{
		Addr:                 transactionCoordinator,
		TransactionalID:      tid,
		TransactionTimeoutMs: 30000,
		ProducerID:           pid1,
		ProducerEpoch:        epoch1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if resp.Error != nil {
		t.Fatal(resp.Error)
	}

	epoch2 := resp.Producer.ProducerEpoch
	pid2 := resp.Producer.ProducerID

	if pid1 != pid2 {
		t.Fatalf("PID should stay the same across producer sessions; expected: %v got: %v", pid1, pid2)
	}

	if epoch2-epoch1 <= 0 {
		t.Fatal("Epoch should increase when producer is initialized again with the same transactionID")
	}

	// Checks if transaction timeout is too high
	// Transaction timeout should never be higher than broker config `transaction.max.timeout.ms`
	resp, _ = client.InitProducerID(context.Background(), &InitProducerIDRequest{
		Addr:                 client.Addr,
		TransactionalID:      tid,
		TransactionTimeoutMs: 30000000,
	})
	if !errors.Is(resp.Error, InvalidTransactionTimeout) {
		t.Fatal("Should have errored with: Transaction timeout specified is higher than `transaction.max.timeout.ms`")
	}
}
