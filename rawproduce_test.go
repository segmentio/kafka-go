package kafka

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestClientRawProduce(t *testing.T) {
	// The RawProduce request records are encoded in the format introduced in Kafka 0.11.0.
	if !ktesting.KafkaIsAtLeast("0.11.0") {
		t.Skip("Skipping because the RawProduce request is not supported by Kafka versions below 0.11.0")
	}

	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	now := time.Now()

	res, err := client.RawProduce(context.Background(), &RawProduceRequest{
		Topic:        topic,
		Partition:    0,
		RequiredAcks: -1,
		RawRecords: NewRawRecordSet(NewRecordReader(
			Record{Time: now, Value: NewBytes([]byte(`hello-1`))},
			Record{Time: now, Value: NewBytes([]byte(`hello-2`))},
			Record{Time: now, Value: NewBytes([]byte(`hello-3`))},
		), 0),
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

func TestClientRawProduceCompressed(t *testing.T) {
	// The RawProduce request records are encoded in the format introduced in Kafka 0.11.0.
	if !ktesting.KafkaIsAtLeast("0.11.0") {
		t.Skip("Skipping because the RawProduce request is not supported by Kafka versions below 0.11.0")
	}

	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	now := time.Now()

	res, err := client.RawProduce(context.Background(), &RawProduceRequest{
		Topic:        topic,
		Partition:    0,
		RequiredAcks: -1,
		RawRecords: NewRawRecordSet(NewRecordReader(
			Record{Time: now, Value: NewBytes([]byte(`hello-1`))},
			Record{Time: now, Value: NewBytes([]byte(`hello-2`))},
			Record{Time: now, Value: NewBytes([]byte(`hello-3`))},
		), protocol.Gzip),
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

func TestClientRawProduceNilRecords(t *testing.T) {
	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	_, err := client.RawProduce(context.Background(), &RawProduceRequest{
		Topic:        topic,
		Partition:    0,
		RequiredAcks: -1,
		RawRecords:   protocol.RawRecordSet{Reader: nil},
	})

	if err != nil {
		t.Fatal(err)
	}
}

func TestClientRawProduceEmptyRecords(t *testing.T) {
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

func NewRawRecordSet(reader protocol.RecordReader, attr protocol.Attributes) protocol.RawRecordSet {
	rs := protocol.RecordSet{Version: 2, Attributes: attr, Records: reader}
	buf := &bytes.Buffer{}
	rs.WriteTo(buf)

	return protocol.RawRecordSet{
		Reader: buf,
	}
}
