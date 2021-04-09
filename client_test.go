package kafka

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/apoorvag-mav/kafka-go/compress"
	ktesting "github.com/apoorvag-mav/kafka-go/testing"
)

func newLocalClientAndTopic() (*Client, string, func()) {
	topic := makeTopic()
	client, shutdown := newLocalClientWithTopic(topic, 1)
	return client, topic, shutdown
}

func newLocalClientWithTopic(topic string, partitions int) (*Client, func()) {
	client, shutdown := newLocalClient()
	if err := clientCreateTopic(client, topic, partitions); err != nil {
		shutdown()
		panic(err)
	}
	return client, func() {
		client.DeleteTopics(context.Background(), &DeleteTopicsRequest{
			Topics: []string{topic},
		})
		shutdown()
	}
}

func clientCreateTopic(client *Client, topic string, partitions int) error {
	_, err := client.CreateTopics(context.Background(), &CreateTopicsRequest{
		Topics: []TopicConfig{{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		}},
	})
	if err != nil {
		return err
	}

	// Topic creation seems to be asynchronous. Metadata for the topic partition
	// layout in the cluster is available in the controller before being synced
	// with the other brokers, which causes "Error:[3] Unknown Topic Or Partition"
	// when sending requests to the partition leaders.
	//
	// This loop will wait up to 2 seconds polling the cluster until no errors
	// are returned.
	for i := 0; i < 20; i++ {
		r, err := client.Fetch(context.Background(), &FetchRequest{
			Topic:     topic,
			Partition: 0,
			Offset:    0,
		})
		if err == nil && r.Error == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

func newLocalClient() (*Client, func()) {
	return newClient(TCP("localhost"))
}

func newClient(addr net.Addr) (*Client, func()) {
	conns := &ktesting.ConnWaitGroup{
		DialFunc: (&net.Dialer{}).DialContext,
	}

	transport := &Transport{
		Dial:     conns.Dial,
		Resolver: NewBrokerResolver(nil),
	}

	client := &Client{
		Addr:      addr,
		Timeout:   5 * time.Second,
		Transport: transport,
	}

	return client, func() { transport.CloseIdleConnections(); conns.Wait() }
}

func TestClient(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T, context.Context, *Client)
	}{
		{
			scenario: "retrieve committed offsets for a consumer group and topic",
			function: testConsumerGroupFetchOffsets,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			client, shutdown := newLocalClient()
			defer shutdown()

			testFunc(t, ctx, client)
		})
	}
}

func testConsumerGroupFetchOffsets(t *testing.T, ctx context.Context, client *Client) {
	const totalMessages = 144
	const partitions = 12
	const msgPerPartition = totalMessages / partitions

	topic := makeTopic()
	if err := clientCreateTopic(client, topic, partitions); err != nil {
		t.Fatal(err)
	}

	groupId := makeGroupID()
	brokers := []string{"localhost:9092"}

	writer := &Writer{
		Addr:      TCP(brokers...),
		Topic:     topic,
		Balancer:  &RoundRobin{},
		BatchSize: 1,
		Transport: client.Transport,
	}
	if err := writer.WriteMessages(ctx, makeTestSequence(totalMessages)...); err != nil {
		t.Fatalf("bad write messages: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("bad write err: %v", err)
	}

	r := NewReader(ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupId,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  100 * time.Millisecond,
	})
	defer r.Close()

	for i := 0; i < totalMessages; i++ {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			t.Fatalf("error fetching message: %s", err)
		}
		if err := r.CommitMessages(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}

	offsets, err := client.ConsumerOffsets(ctx, TopicAndGroup{GroupId: groupId, Topic: topic})
	if err != nil {
		t.Fatal(err)
	}

	if len(offsets) != partitions {
		t.Fatalf("expected %d partitions but only received offsets for %d", partitions, len(offsets))
	}

	for i := 0; i < partitions; i++ {
		committedOffset := offsets[i]
		if committedOffset != msgPerPartition {
			t.Errorf("expected partition %d with committed offset of %d but received %d", i, msgPerPartition, committedOffset)
		}
	}
}

func TestClientProduceAndConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Tests a typical kafka use case, data is produced to a partition,
	// then consumed back sequentially. We use snappy compression because
	// kafka stream are often compressed, and verify that each record
	// produced is exposed to the consumer, and order is preserved.
	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	epoch := time.Now()
	seed := int64(0) // deterministic
	prng := rand.New(rand.NewSource(seed))
	offset := int64(0)

	const numBatches = 100
	const recordsPerBatch = 320
	t.Logf("producing %d batches of %d records...", numBatches, recordsPerBatch)

	for i := 0; i < numBatches; i++ { // produce 100 batches
		records := make([]Record, recordsPerBatch)

		for i := range records {
			v := make([]byte, prng.Intn(999)+1)
			io.ReadFull(prng, v)
			records[i].Time = epoch
			records[i].Value = NewBytes(v)
		}

		res, err := client.Produce(ctx, &ProduceRequest{
			Topic:        topic,
			Partition:    0,
			RequiredAcks: -1,
			Records:      NewRecordReader(records...),
			Compression:  compress.Snappy,
		})
		if err != nil {
			t.Fatal(err)
		}
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		if res.BaseOffset != offset {
			t.Fatalf("records were produced at an unexpected offset, want %d but got %d", offset, res.BaseOffset)
		}
		offset += int64(len(records))
	}

	prng.Seed(seed)
	offset = 0 // reset
	numFetches := 0
	numRecords := 0

	for numRecords < (numBatches * recordsPerBatch) {
		res, err := client.Fetch(ctx, &FetchRequest{
			Topic:     topic,
			Partition: 0,
			Offset:    offset,
			MinBytes:  1,
			MaxBytes:  256 * 1024,
			MaxWait:   100 * time.Millisecond, // should only hit on the last fetch
		})
		if err != nil {
			t.Fatal(err)
		}
		if res.Error != nil {
			t.Fatal(err)
		}

		for {
			r, err := res.Records.ReadRecord()
			if err != nil {
				if err != io.EOF {
					t.Fatal(err)
				}
				break
			}

			if r.Key != nil {
				r.Key.Close()
				t.Error("unexpected non-null key on record at offset", r.Offset)
			}

			n := prng.Intn(999) + 1
			a := make([]byte, n)
			b := make([]byte, n)
			io.ReadFull(prng, a)

			_, err = io.ReadFull(r.Value, b)
			r.Value.Close()
			if err != nil {
				t.Fatalf("reading record at offset %d: %v", r.Offset, err)
			}

			if !bytes.Equal(a, b) {
				t.Fatalf("value of record at offset %d mismatches", r.Offset)
			}

			if r.Offset != offset {
				t.Fatalf("record at offset %d was expected to have offset %d", r.Offset, offset)
			}

			offset = r.Offset + 1
			numRecords++
		}

		numFetches++
	}

	t.Logf("%d records were read in %d fetches", numRecords, numFetches)
}
