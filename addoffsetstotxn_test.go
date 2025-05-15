package kafka

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestClientAddOffsetsToTxn(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("0.11.0") {
		t.Skip("Skipping test because kafka version is not high enough.")
	}

	// TODO: look into why this test fails on Kafka 3.0.0 and higher when transactional support
	// work is revisited.
	if ktesting.KafkaIsAtLeast("3.0.0") {
		t.Skip("Skipping test because it fails on Kafka version 3.0.0 or higher.")
	}

	topic := makeTopic()
	transactionalID := makeTransactionalID()
	client, shutdown := newLocalClient()
	defer shutdown()

	err := clientCreateTopic(client, topic, 3)
	defer deleteTopic(t, topic)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	waitForTopic(ctx, t, topic)
	defer cancel()
	respc, err := waitForCoordinatorIndefinitely(ctx, client, &FindCoordinatorRequest{
		Addr:    client.Addr,
		Key:     transactionalID,
		KeyType: CoordinatorKeyTypeConsumer,
	})
	if err != nil {
		t.Fatal(err)
	}

	if respc.Error != nil {
		t.Fatal(err)
	}

	groupID := makeGroupID()

	group, err := NewConsumerGroup(ConsumerGroupConfig{
		ID:                groupID,
		Topics:            []string{topic},
		Brokers:           []string{"localhost:9092"},
		HeartbeatInterval: 2 * time.Second,
		RebalanceTimeout:  2 * time.Second,
		RetentionTime:     time.Hour,
		Logger:            log.New(os.Stdout, "cg-test: ", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	_, err = group.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	respc, err = waitForCoordinatorIndefinitely(ctx, client, &FindCoordinatorRequest{
		Addr:    client.Addr,
		Key:     transactionalID,
		KeyType: CoordinatorKeyTypeTransaction,
	})
	if err != nil {
		t.Fatal(err)
	}

	if respc.Error != nil {
		t.Fatal(err)
	}

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

	resp, err := client.AddOffsetsToTxn(ctx, &AddOffsetsToTxnRequest{
		TransactionalID: transactionalID,
		ProducerID:      ipResp.Producer.ProducerID,
		ProducerEpoch:   ipResp.Producer.ProducerEpoch,
		GroupID:         groupID,
	})
	if err != nil {
		t.Fatal(err)
	}

	if resp.Error != nil {
		t.Fatal(err)
	}
}
