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

func TestClientDeleteOffset(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("2.4.0") {
		return
	}

	topic := makeTopic()
	client, shutdown := newLocalClientWithTopic(topic, 3)
	defer shutdown()
	now := time.Now()

	const N = 10 * 3
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

	gen, err := group.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ocr, err := client.OffsetCommit(ctx, &OffsetCommitRequest{
		Addr:         nil,
		GroupID:      groupID,
		GenerationID: int(gen.ID),
		MemberID:     gen.MemberID,
		Topics: map[string][]OffsetCommit{
			topic: {
				{Partition: 0, Offset: 10},
				{Partition: 1, Offset: 10},
				{Partition: 2, Offset: 10},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	group.Close()

	resps := ocr.Topics[topic]
	if len(resps) != 3 {
		t.Fatalf("expected 3 offsetcommitpartition responses; got %d", len(resps))
	}

	for _, resp := range resps {
		if resp.Error != nil {
			t.Fatal(resp.Error)
		}
	}

	ofr, err := client.OffsetFetch(ctx, &OffsetFetchRequest{
		GroupID: groupID,
		Topics:  map[string][]int{topic: {0, 1, 2}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if ofr.Error != nil {
		t.Error(res.Error)
	}

	fetresps := ofr.Topics[topic]
	if len(fetresps) != 3 {
		t.Fatalf("expected 3 offsetfetchpartition responses; got %d", len(fetresps))
	}

	for _, r := range fetresps {
		if r.Error != nil {
			t.Fatal(r.Error)
		}

		if r.CommittedOffset != 10 {
			t.Fatalf("expected committed offset to be 10; got: %v for partition: %v", r.CommittedOffset, r.Partition)
		}
	}

	// Remove offsets
	odr, err := client.OffsetDelete(ctx, &OffsetDeleteRequest{
		GroupID: groupID,
		Topics:  map[string][]int{topic: {0, 1, 2}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if odr.Error != nil {
		t.Error(odr.Error)
	}

	// Fetch the offsets again
	ofr, err = client.OffsetFetch(ctx, &OffsetFetchRequest{
		GroupID: groupID,
		Topics:  map[string][]int{topic: {0, 1, 2}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if ofr.Error != nil {
		t.Error(res.Error)
	}

	for _, r := range ofr.Topics[topic] {
		if r.CommittedOffset != -1 {
			t.Fatalf("expected committed offset to be -1; got: %v for partition: %v", r.CommittedOffset, r.Partition)
		}
	}
}
