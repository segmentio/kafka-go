package kafka

import (
	"bufio"
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestOffsetFetchResponseV1(t *testing.T) {
	item := offsetFetchResponseV1{
		Responses: []offsetFetchResponseV1Response{
			{
				Topic: "a",
				PartitionResponses: []offsetFetchResponseV1PartitionResponse{
					{
						Partition: 2,
						Offset:    3,
						Metadata:  "b",
						ErrorCode: 4,
					},
				},
			},
		},
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found offsetFetchResponseV1
	remain, err := (&found).readFrom(bufio.NewReader(b), b.Len())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if remain != 0 {
		t.Errorf("expected 0 remain, got %v", remain)
		t.FailNow()
	}
	if !reflect.DeepEqual(item, found) {
		t.Error("expected item and found to be the same")
		t.FailNow()
	}
}

func TestOffsetFetchRequestWithNoTopic(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("0.10.2.0") {
		t.Logf("Test %s is not applicable for kafka versions below 0.10.2.0", t.Name())
		t.SkipNow()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	topic1 := makeTopic()
	defer deleteTopic(t, topic1)
	topic2 := makeTopic()
	defer deleteTopic(t, topic2)
	consumeGroup := makeGroupID()
	numMsgs := 50
	defer cancel()
	r1 := NewReader(ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic1,
		GroupID:  consumeGroup,
		MinBytes: 1,
		MaxBytes: 100,
		MaxWait:  100 * time.Millisecond,
	})
	defer r1.Close()
	prepareReader(t, ctx, r1, makeTestSequence(numMsgs)...)
	r2 := NewReader(ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic2,
		GroupID:  consumeGroup,
		MinBytes: 1,
		MaxBytes: 100,
		MaxWait:  100 * time.Millisecond,
	})
	defer r2.Close()
	prepareReader(t, ctx, r2, makeTestSequence(numMsgs)...)

	for i := 0; i < numMsgs; i++ {
		if _, err := r1.ReadMessage(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < numMsgs; i++ {
		if _, err := r2.ReadMessage(ctx); err != nil {
			t.Fatal(err)
		}
	}

	client := Client{Addr: TCP("localhost:9092")}

	topicOffsets, err := client.OffsetFetch(ctx, &OffsetFetchRequest{GroupID: consumeGroup})

	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if len(topicOffsets.Topics) != 2 {
		t.Error(err)
		t.FailNow()
	}

}

func TestOffsetFetchRequestWithOneTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	topic1 := makeTopic()
	defer deleteTopic(t, topic1)
	topic2 := makeTopic()
	defer deleteTopic(t, topic2)
	consumeGroup := makeGroupID()
	numMsgs := 50
	defer cancel()
	r1 := NewReader(ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic1,
		GroupID:  consumeGroup,
		MinBytes: 1,
		MaxBytes: 100,
		MaxWait:  100 * time.Millisecond,
	})
	defer r1.Close()
	prepareReader(t, ctx, r1, makeTestSequence(numMsgs)...)
	r2 := NewReader(ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic2,
		GroupID:  consumeGroup,
		MinBytes: 1,
		MaxBytes: 100,
		MaxWait:  100 * time.Millisecond,
	})
	defer r2.Close()
	prepareReader(t, ctx, r2, makeTestSequence(numMsgs)...)

	for i := 0; i < numMsgs; i++ {
		if _, err := r1.ReadMessage(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < numMsgs; i++ {
		if _, err := r2.ReadMessage(ctx); err != nil {
			t.Fatal(err)
		}
	}

	client := Client{Addr: TCP("localhost:9092")}
	topicOffsets, err := client.OffsetFetch(ctx, &OffsetFetchRequest{GroupID: consumeGroup, Topics: map[string][]int{
		topic1: {0},
	}})

	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if len(topicOffsets.Topics) != 1 {
		t.Error(err)
		t.FailNow()
	}
}
