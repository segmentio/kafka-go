package kafka

import (
	"context"
	"io/ioutil"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/compress"
)

func produceRecords(t *testing.T, n int, addr net.Addr, topic string, compression compress.Codec) []Record {
	network := addr.Network()
	address := net.JoinHostPort(addr.String(), "9092")

	conn, err := (&Dialer{
		Resolver: &net.Resolver{},
	}).DialLeader(context.Background(), network, address, topic, 0)

	if err != nil {
		t.Fatal("failed to open a new kafka connection:", err)
	}
	defer conn.Close()

	msgs := makeTestSequence(n)
	if compression == nil {
		_, err = conn.WriteMessages(msgs...)
	} else {
		_, err = conn.WriteCompressedMessages(compression, msgs...)
	}
	if err != nil {
		t.Fatal(err)
	}

	records := make([]Record, len(msgs))
	for offset, msg := range msgs {
		records[offset] = NewRecord(
			int64(offset), time.Time{}, msg.Key, msg.Value, msg.Headers...,
		)
	}

	return records
}

func TestClientFetch(t *testing.T) {
	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	records := produceRecords(t, 10, client.Addr, topic, nil)

	res, err := client.Fetch(context.Background(), &FetchRequest{
		Topic:     topic,
		Partition: 0,
		Offset:    0,
		MinBytes:  1,
		MaxBytes:  64 * 1024,
		MaxWait:   100 * time.Millisecond,
	})

	if err != nil {
		t.Fatal(err)
	}

	assertFetchResponse(t, res, &FetchResponse{
		Topic:         topic,
		Partition:     0,
		HighWatermark: 10,
		Records:       NewRecordSet(records...),
	})
}

func TestClientFetchCompressed(t *testing.T) {
	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	records := produceRecords(t, 10, client.Addr, topic, &compress.GzipCodec)

	res, err := client.Fetch(context.Background(), &FetchRequest{
		Topic:     topic,
		Partition: 0,
		Offset:    0,
		MinBytes:  1,
		MaxBytes:  64 * 1024,
		MaxWait:   100 * time.Millisecond,
	})

	if err != nil {
		t.Fatal(err)
	}

	assertFetchResponse(t, res, &FetchResponse{
		Topic:         topic,
		Partition:     0,
		HighWatermark: 10,
		Records:       NewRecordSet(records...),
	})
}

func assertFetchResponse(t *testing.T, found, expected *FetchResponse) {
	t.Helper()

	if found.Topic != expected.Topic {
		t.Error("invalid topic found in response:", found.Topic)
	}

	if found.Partition != expected.Partition {
		t.Error("invalid partition found in response:", found.Partition)
	}

	if found.HighWatermark != expected.HighWatermark {
		t.Error("invalid high watermark found in response:", found.HighWatermark)
	}

	if found.Error != nil {
		t.Error("unexpected error found in response:", found.Error)
	}

	records1, err := readRecords(found.Records)
	if err != nil {
		t.Error("error reading records:", err)
	}

	records2, err := readRecords(expected.Records)
	if err != nil {
		t.Error("error reading records:", err)
	}

	assertRecords(t, records1, records2)
}

type inMemoryRecord struct {
	offset  int64
	key     []byte
	value   []byte
	headers []Header
}

func assertRecords(t *testing.T, found, expected []inMemoryRecord) {
	t.Helper()
	i := 0

	for i < len(found) && i < len(expected) {
		r1 := found[i]
		r2 := expected[i]

		if !reflect.DeepEqual(r1, r2) {
			t.Errorf("records at index %d don't match", i)
			t.Logf("expected:\n%+v", r2)
			t.Logf("found:\n%+v", r1)
		}

		i++
	}

	for i < len(found) {
		t.Errorf("unexpected record at index %d:\n%+v", i, found[i])
		i++
	}

	for i < len(expected) {
		t.Errorf("missing record at index %d:\n%+v", i, expected[i])
		i++
	}
}

func readRecords(records RecordSet) ([]inMemoryRecord, error) {
	list := []inMemoryRecord{}

	for rec := records.Next(); rec != nil; rec = records.Next() {
		var (
			offset      = rec.Offset()
			key         = rec.Key()
			value       = rec.Value()
			headers     = rec.Headers()
			bytesKey    []byte
			bytesValues []byte
		)

		if key != nil {
			bytesKey, _ = ioutil.ReadAll(key)
		}

		if value != nil {
			bytesValues, _ = ioutil.ReadAll(value)
		}

		list = append(list, inMemoryRecord{
			offset:  offset,
			key:     bytesKey,
			value:   bytesValues,
			headers: headers,
		})
	}

	return list, records.Close()
}
