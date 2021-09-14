package kafka

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestClientAddPartitionsToTxn(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("0.11.0") {
		t.Skip("Skipping test because kafka version is not high enough.")
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
			ProducerEpoch:   ipResp.Producer.ProducerID,
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
