package kafka

import (
	"bytes"
	"context"
	"github.com/segmentio/kafka-go/protocol"
	"testing"
	"time"
)

func TestClientRawProduce(t *testing.T) {
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
		), 2, 0),
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
		), 2, protocol.Gzip),
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

func NewRawRecordSet(reader protocol.RecordReader, version int8, attr protocol.Attributes) protocol.RawRecordSet {
	rs := protocol.RecordSet{Version: version, Attributes: attr, Records: reader}
	buf := &bytes.Buffer{}
	rs.WriteTo(buf)

	return protocol.RawRecordSet{
		Version: version,
		Reader:  buf,
	}
}
