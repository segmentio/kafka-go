package kafka

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/compress"
)

func TestRequiredAcks(t *testing.T) {
	for _, acks := range []RequiredAcks{
		RequireNone,
		RequireOne,
		RequireAll,
	} {
		t.Run(acks.String(), func(t *testing.T) {
			a := strconv.Itoa(int(acks))
			x := RequiredAcks(-2)
			y := RequiredAcks(-2)
			b, err := acks.MarshalText()
			if err != nil {
				t.Fatal(err)
			}

			if err := x.UnmarshalText([]byte(a)); err != nil {
				t.Fatal(err)
			}
			if err := y.UnmarshalText(b); err != nil {
				t.Fatal(err)
			}

			if x != acks {
				t.Errorf("required acks mismatch after marshal/unmarshal text: want=%s got=%s", acks, x)
			}
			if y != acks {
				t.Errorf("required acks mismatch after marshal/unmarshal value: want=%s got=%s", acks, y)
			}
		})
	}
}

func TestClientProduce(t *testing.T) {
	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	now := time.Now()

	res, err := client.Produce(context.Background(), &ProduceRequest{
		Topic:        topic,
		Partition:    0,
		RequiredAcks: -1,
		Records: NewRecordReader(
			Record{Time: now, Value: NewBytes([]byte(`hello-1`))},
			Record{Time: now, Value: NewBytes([]byte(`hello-2`))},
			Record{Time: now, Value: NewBytes([]byte(`hello-3`))},
		),
	})

	if err != nil {
		t.Fatal(err)
	}

	if res.Error != nil {
		t.Error(res.Error)
	}

	for index, err := range res.RecordErrors {
		t.Errorf("record at index %d produced an error: %v", index, err)
	}
}

func TestClientProduceCompressed(t *testing.T) {
	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	now := time.Now()

	res, err := client.Produce(context.Background(), &ProduceRequest{
		Topic:        topic,
		Partition:    0,
		RequiredAcks: -1,
		Compression:  compress.Gzip,
		Records: NewRecordReader(
			Record{Time: now, Value: NewBytes([]byte(`hello-1`))},
			Record{Time: now, Value: NewBytes([]byte(`hello-2`))},
			Record{Time: now, Value: NewBytes([]byte(`hello-3`))},
		),
	})

	if err != nil {
		t.Fatal(err)
	}

	if res.Error != nil {
		t.Error(res.Error)
	}

	for index, err := range res.RecordErrors {
		t.Errorf("record at index %d produced an error: %v", index, err)
	}
}

func TestClientProduceNilRecords(t *testing.T) {
	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	_, err := client.Produce(context.Background(), &ProduceRequest{
		Topic:        topic,
		Partition:    0,
		RequiredAcks: -1,
		Records:      nil,
	})

	if err != nil {
		t.Fatal(err)
	}
}

func TestClientProduceEmptyRecords(t *testing.T) {
	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	_, err := client.Produce(context.Background(), &ProduceRequest{
		Topic:        topic,
		Partition:    0,
		RequiredAcks: -1,
		Records:      NewRecordReader(),
	})

	if err != nil {
		t.Fatal(err)
	}
}
