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
		Records: NewRecordBatch(
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
		Records: NewRecordBatch(
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
