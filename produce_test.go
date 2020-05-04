package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/compress"
)

func TestClientProduce(t *testing.T) {
	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	now := time.Now()

	res, err := client.Produce(context.Background(), &ProduceRequest{
		Topic:        topic,
		Partition:    0,
		RequiredAcks: -1,
		Records: NewRecordSet(
			NewRecord(0, now, nil, []byte(`hello-1`)),
			NewRecord(0, now, nil, []byte(`hello-2`)),
			NewRecord(0, now, nil, []byte(`hello-3`)),
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
		Records: NewRecordSet(
			NewRecord(0, now, nil, []byte(`hello-1`)),
			NewRecord(0, now, nil, []byte(`hello-2`)),
			NewRecord(0, now, nil, []byte(`hello-3`)),
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
