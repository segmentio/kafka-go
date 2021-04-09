package kafka

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/apoorvag-mav/kafka-go/compress"
)

func copyRecords(records []Record) []Record {
	newRecords := make([]Record, len(records))

	for i := range records {
		k, _ := ReadAll(records[i].Key)
		v, _ := ReadAll(records[i].Value)

		records[i].Key = NewBytes(k)
		records[i].Value = NewBytes(v)

		newRecords[i].Key = NewBytes(k)
		newRecords[i].Value = NewBytes(v)
	}

	return newRecords
}

func produceRecords(t *testing.T, n int, addr net.Addr, topic string, compression compress.Codec) []Record {
	conn, err := (&Dialer{
		Resolver: &net.Resolver{},
	}).DialLeader(context.Background(), addr.Network(), addr.String(), topic, 0)

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
		records[offset] = Record{
			Offset:  int64(offset),
			Key:     NewBytes(msg.Key),
			Value:   NewBytes(msg.Value),
			Headers: msg.Headers,
		}
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
		Records:       NewRecordReader(records...),
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
		Records:       NewRecordReader(records...),
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

type memoryRecord struct {
	offset  int64
	key     []byte
	value   []byte
	headers []Header
}

func assertRecords(t *testing.T, found, expected []memoryRecord) {
	t.Helper()
	i := 0

	for i < len(found) && i < len(expected) {
		r1 := found[i]
		r2 := expected[i]

		if !reflect.DeepEqual(r1, r2) {
			t.Errorf("records at index %d don't match", i)
			t.Logf("expected:\n%#v", r2)
			t.Logf("found:\n%#v", r1)
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

func readRecords(records RecordReader) ([]memoryRecord, error) {
	list := []memoryRecord{}

	for {
		rec, err := records.ReadRecord()

		if err != nil {
			if errors.Is(err, io.EOF) {
				return list, nil
			}
			return nil, err
		}

		var (
			offset      = rec.Offset
			key         = rec.Key
			value       = rec.Value
			headers     = rec.Headers
			bytesKey    []byte
			bytesValues []byte
		)

		if key != nil {
			bytesKey, _ = ioutil.ReadAll(key)
		}

		if value != nil {
			bytesValues, _ = ioutil.ReadAll(value)
		}

		list = append(list, memoryRecord{
			offset:  offset,
			key:     bytesKey,
			value:   bytesValues,
			headers: headers,
		})
	}
}

func TestClientPipeline(t *testing.T) {
	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	const numBatches = 100
	const recordsPerBatch = 30

	unixEpoch := time.Unix(0, 0)
	records := make([]Record, recordsPerBatch)
	content := []byte("1234567890")

	for i := 0; i < numBatches; i++ {
		for j := range records {
			records[j] = Record{Value: NewBytes(content)}
		}

		_, err := client.Produce(context.Background(), &ProduceRequest{
			Topic:        topic,
			RequiredAcks: -1,
			Records:      NewRecordReader(records...),
			Compression:  Snappy,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	offset := int64(0)

	for i := 0; i < (numBatches * recordsPerBatch); {
		req := &FetchRequest{
			Topic:    topic,
			Offset:   offset,
			MinBytes: 1,
			MaxBytes: 8192,
			MaxWait:  500 * time.Millisecond,
		}

		res, err := client.Fetch(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}

		if res.Error != nil {
			t.Fatal(res.Error)
		}

		for {
			r, err := res.Records.ReadRecord()
			if err != nil {
				if err == io.EOF {
					break
				}
				t.Fatal(err)
			}

			if r.Key != nil {
				r.Key.Close()
			}

			if r.Value != nil {
				r.Value.Close()
			}

			if r.Offset != offset {
				t.Errorf("record at index %d has mismatching offset, want %d but got %d", i, offset, r.Offset)
			}

			if r.Time.IsZero() || r.Time.Equal(unixEpoch) {
				t.Errorf("record at index %d with offset %d has not timestamp", i, r.Offset)
			}

			offset = r.Offset + 1
			i++
		}
	}
}
