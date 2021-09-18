package kafka

import (
	"context"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestClientTxnOffsetCommit(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("0.11.0") {
		t.Skip("Skipping test because kafka version is not high enough.")
	}

	transactionalID := makeTransactionalID()
	topic := makeTopic()

	client, shutdown := newLocalClientWithTopic(topic, 1)
	defer shutdown()

	now := time.Now()

	const N = 10
	records := make([]Record, 0, N)
	for i := 0; i < N; i++ {
		records = append(records, Record{
			Time:  now,
			Value: NewBytes([]byte("test-message-" + strconv.Itoa(i))),
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	res, err := client.Produce(ctx, &ProduceRequest{
		Topic:        topic,
		RequiredAcks: RequireAll,
		Records:      NewRecordReader(records...),
	})
	if err != nil {
		t.Fatal(err)
	}

	if res.Error != nil {
		t.Error(res.Error)
	}

	for index, err := range res.RecordErrors {
		t.Fatalf("record at index %d produced an error: %v", index, err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	respc, err := waitForCoordinatorIndefinitely(ctx, client, &FindCoordinatorRequest{
		Addr:    client.Addr,
		Key:     transactionalID,
		KeyType: CoordinatorKeyTypeTransaction,
	})
	if err != nil {
		t.Fatal(err)
	}

	if respc.Error != nil {
		t.Fatal(respc.Error)
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	respc, err = waitForCoordinatorIndefinitely(ctx, client, &FindCoordinatorRequest{
		Addr:    client.Addr,
		Key:     transactionalID,
		KeyType: CoordinatorKeyTypeConsumer,
	})
	if err != nil {
		t.Fatal(err)
	}

	if respc.Error != nil {
		t.Fatal(respc.Error)
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
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
	gen, err := group.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	apresp, err := client.AddPartitionsToTxn(ctx, &AddPartitionsToTxnRequest{
		TransactionalID: transactionalID,
		ProducerID:      ipResp.Producer.ProducerID,
		ProducerEpoch:   ipResp.Producer.ProducerEpoch,
		Topics: map[string][]AddPartitionToTxn{
			topic: {
				{
					Partition: 0,
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	appartition := apresp.Topics[topic]
	if len(appartition) != 1 {
		t.Fatalf("unexpected partition count; expected: 1, got: %d", len(appartition))
	}

	for _, partition := range appartition {
		if partition.Error != nil {
			t.Fatal(partition.Error)
		}
	}

	client.AddOffsetsToTxn(ctx, &AddOffsetsToTxnRequest{
		TransactionalID: transactionalID,
		ProducerID:      ipResp.Producer.ProducerID,
		ProducerEpoch:   ipResp.Producer.ProducerEpoch,
		GroupID:         groupID,
	})

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	resp, err := client.TxnOffsetCommit(ctx, &TxnOffsetCommitRequest{
		TransactionalID: transactionalID,
		GroupID:         groupID,
		MemberID:        gen.MemberID,
		ProducerID:      ipResp.Producer.ProducerID,
		ProducerEpoch:   ipResp.Producer.ProducerEpoch,
		GenerationID:    int(gen.ID),
		GroupInstanceID: groupID,
		Topics: map[string][]TxnOffsetCommit{
			topic: {
				{
					Partition: 0,
					Offset:    10,
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	partitions := resp.Topics[topic]

	if len(partitions) != 1 {
		t.Fatalf("unexpected partition count; expected: 1, got: %d", len(partitions))
	}

	for _, partition := range partitions {
		if partition.Error != nil {
			t.Fatal(partition.Error)
		}
	}

	err = clientEndTxn(client, &EndTxnRequest{
		TransactionalID: transactionalID,
		ProducerID:      ipResp.Producer.ProducerID,
		ProducerEpoch:   ipResp.Producer.ProducerEpoch,
		Committed:       true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// seems like external visibility of the commit isn't
	// synchronous with the EndTxn request. This seems
	// to give enough time for the commit to become consistently visible.
	<-time.After(time.Second)

	ofr, err := client.OffsetFetch(ctx, &OffsetFetchRequest{
		GroupID: groupID,
		Topics:  map[string][]int{topic: {0}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if ofr.Error != nil {
		t.Error(ofr.Error)
	}

	fetresps := ofr.Topics[topic]
	if len(fetresps) != 1 {
		t.Fatalf("unexpected 1 offsetfetchpartition responses; got %d", len(fetresps))
	}

	for _, r := range fetresps {
		if r.Error != nil {
			t.Fatal(r.Error)
		}

		if r.CommittedOffset != 10 {
			t.Fatalf("expected committed offset to be 10; got: %v for partition: %v", r.CommittedOffset, r.Partition)
		}
	}
}
