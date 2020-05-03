package kafka

import (
	"bytes"
	"context"
	"io/ioutil"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestClientFetch(t *testing.T) {
	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	msgs := makeTestSequence(10)
	conn, err := (&Dialer{
		Resolver: &net.Resolver{},
	}).DialLeader(context.Background(), "tcp", "localhost:9092", topic, 0)
	if err != nil {
		t.Fatal("failed to open a new kafka connection:", err)
	}
	defer conn.Close()

	if _, err := conn.WriteMessages(msgs...); err != nil {
		t.Fatal(err)
	}

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

	if res.Topic != topic {
		t.Error("invalid topic found in response:", res.Topic)
	}

	if res.Partition != 0 {
		t.Error("invalid partition found in response:", res.Partition)
	}

	if res.HighWatermark != 10 {
		t.Error("invalid high watermark found in response:", res.HighWatermark)
	}

	if res.Error != nil {
		t.Error("unexpected error found in response:", res.Error)
	}

	i := 0

	for r := res.Records.Next(); r != nil; r = res.Records.Next() {
		m := &msgs[i]

		if r.Offset() != int64(i) {
			t.Errorf("invalid record offset at index %d: offset=%d", i, r.Offset())
		}

		if r.Key() == nil {
			if m.Key != nil {
				t.Errorf("unexpected nil record key at index %d", i)
			}
		} else {
			k, err := ioutil.ReadAll(r.Key())
			if err != nil {
				t.Errorf("unexpected error reading record key at index %d: %v", i, err)
			}
			if !bytes.Equal(k, m.Key) {
				t.Errorf("invalid record key found at index %d: %q", i, k)
			}
		}

		if r.Value() == nil {
			if m.Value != nil {
				t.Errorf("unexpected nil record value at index %d", i)
			}
		} else {
			v, err := ioutil.ReadAll(r.Value())
			if err != nil {
				t.Errorf("unexpected error reading record value at index %d: %v", i, err)
			}
			if !bytes.Equal(v, m.Value) {
				t.Errorf("invalid record value found at index %d: %q", i, v)
			}
		}

		if !reflect.DeepEqual(r.Headers(), m.Headers) {
			t.Errorf("invalid record headers at index %d", i)
			t.Log("expected:", m.Headers)
			t.Log("found:   ", r.Headers())
		}

		i++
	}

	if i != len(msgs) {
		t.Errorf("fetch response only returned %d records even tho %d were produced to the partition", i, len(msgs))
	}

	if err := res.Records.Close(); err != nil {
		t.Error("unexpected error closing the record set:", err)
	}
}
