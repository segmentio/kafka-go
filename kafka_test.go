package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
)

type Producer func(t *testing.T, msg []byte) int64

func produce(t *testing.T, topic string, msg []byte) int64 {
	producer := newProducer(t)
	defer producer.Close()

	message := sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msg),
	}

	_, offset, err := producer.SendMessage(&message)
	if err != nil {
		t.Fatalf("send message errored: %s", err.Error())
	}

	return offset
}

func makeProducer(t *testing.T, topic string) func(t *testing.T, msg []byte) int64 {
	return func(t *testing.T, msg []byte) int64 {
		return produce(t, topic, msg)
	}
}

func newProducer(t *testing.T) sarama.SyncProducer {
	conf := sarama.NewConfig()
	conf.Version = sarama.V0_10_0_0
	conf.Producer.Return.Successes = true

	client, err := sarama.NewClient([]string{"localhost:9092"}, conf)
	if err != nil {
		t.Fatalf("error trying to create client: %s", err.Error())
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		t.Fatalf("error trying to create producer: %s", err.Error())
	}

	return producer
}

func TestReader(t *testing.T) {
	tests := []struct {
		scenario string
		test     func(t *testing.T, ctx context.Context, reader Reader, producer Producer)
	}{
		{
			"should close without errors",
			closeNoErrors,
		},

		{
			"Read should cancel with expired context",
			readCancelContext,
		},

		{
			"Seek return newest offset",
			seekNewestOffset,
		},

		{
			"Seek return oldest offset",
			seekOldestOffset,
		},

		{
			"seek and read oldest offset",
			seekReadOldestOffset,
		},

		{
			"seek and read newest offset",
			seekReadNewestOffset,
		},

		{
			"sequentially consume a reader",
			testReaderConsume,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			topic := uuid.New().String()

			config := ReaderConfig{
				Brokers:            []string{"localhost:9092"},
				Topic:              topic,
				Partition:          0,
				RequestMaxWaitTime: 100 * time.Millisecond,
				RequestMinBytes:    100,
			}

			reader, err := NewReader(config)
			if err != nil {
				t.Fatalf("error creating reader: %s", err.Error())
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			producer := makeProducer(t, topic)

			test.test(t, ctx, reader, producer)
		})
	}
}

func closeNoErrors(t *testing.T, ctx context.Context, reader Reader, producer Producer) {
	err := reader.Close()
	if err != nil {
		t.Fatalf("unexpected error while closing the reader: %s", err.Error())
	}
}

func readCancelContext(t *testing.T, ctx context.Context, reader Reader, producer Producer) {
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	_, err := reader.Read(ctx)
	if err != context.Canceled {
		t.Fatalf("unexpected error while reading: %s", err.Error())
	}
}

func seekNewestOffset(t *testing.T, ctx context.Context, reader Reader, producer Producer) {
	offset := producer(t, []byte("foobar xxx"))

	newOffset, err := reader.Seek(ctx, -1)
	if err != nil {
		t.Fatalf("seek returned an error: %s", err.Error())
	}

	if newOffset != offset {
		t.Fatalf("offsets do not match. expected %d, got %d", offset, newOffset)
	}

	reader.Close()
}

func seekOldestOffset(t *testing.T, ctx context.Context, reader Reader, producer Producer) {
	offset, err := reader.Seek(ctx, -2)
	if err != nil {
		t.Fatalf("seek returned an error: %s", err.Error())
	}

	if offset != 0 {
		t.Fatalf("offsets do not match. expected offset of 0, got %d", offset)
	}

	reader.Close()
}

func seekReadOldestOffset(t *testing.T, ctx context.Context, reader Reader, producer Producer) {
	offset := producer(t, []byte("foobar xxx"))

	_, err := reader.Seek(ctx, -2)
	if err != nil {
		t.Fatalf("seek returned an error: %s", err.Error())
	}

	msg, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("read returned an error: %s", err.Error())
	}

	if string(msg.Value) != "foobar xxx" {
		t.Fatalf("unexpected message value")
	}

	if offset != msg.Offset {
		t.Fatalf("offsets do not match")
	}

	reader.Close()
}

func seekReadNewestOffset(t *testing.T, ctx context.Context, reader Reader, producer Producer) {
	lastOffset := producer(t, []byte("hello 123"))

	_, err := reader.Seek(ctx, -1)
	if err != nil {
		t.Fatalf("seek returned an error: %s", err.Error())
	}

	msgLast, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("read returned an error: %s", err.Error())
	}

	_, err = reader.Seek(ctx, lastOffset)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	msg, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("read returned an error: %s", err.Error())
	}

	if msg.Offset != msgLast.Offset {
		t.Fatalf("newest message doesn't match newest offset. expected %d, got %d", msg.Offset, msgLast.Offset)
	}

	reader.Close()
}

func testReaderConsume(t *testing.T, ctx context.Context, reader Reader, producer Producer) {
	defer reader.Close()

	msgs := make([][]byte, 168)
	for i := range msgs {
		msgs[i] = []byte(fmt.Sprintf("consume job/%03d", i))
		producer(t, msgs[i])
	}

	cur := reader.Offset()

	for i, _ := range msgs {
		if cur2, err := reader.Seek(ctx, cur); err != nil {
			t.Error("seeking to the current position failed:", err)
			return
		} else if cur != cur2 {
			t.Error("seeking to the current position should have produced the same cursor:")
			t.Logf("expected: %q", cur)
			t.Logf("found:    %q", cur2)
			return
		}

		msg, err := reader.Read(ctx)
		if err != nil {
			t.Errorf("reading the msg at %q from the stream failed: %s", cur, err)
			return
		}

		if string(msgs[i]) != string(msg.Value) {
			t.Errorf("the msg returned at %d doesn't match:", cur)
			t.Logf("expected: %#v", string(msgs[i]))
			t.Logf("found:    %#v", string(msg.Value))
			return
		}

		cur = reader.Offset() + 1
	}
}
