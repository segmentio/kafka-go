package kafka

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

func newLocalClientAndTopic() (*Client, string, func()) {
	topic := makeTopic()
	client, shutdown := newClient(TCP("localhost"))

	_, err := client.CreateTopics(context.Background(), &CreateTopicsRequest{
		Topics: []TopicConfig{{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}},
	})
	if err != nil {
		shutdown()
		panic(err)
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

	return client, topic, func() {
		client.DeleteTopics(context.Background(), &DeleteTopicsRequest{
			Topics: []string{topic},
		})
		shutdown()
	}
}

func newLocalClient() (*Client, func()) {
	return newClient(TCP("localhost"))
}

func newClient(addr net.Addr) (*Client, func()) {
	conns := &connGroup{
		dial: (&net.Dialer{}).DialContext,
	}

	transport := &Transport{
		Dial: conns.Dial,
	}

	client := &Client{
		Addr:      addr,
		Timeout:   5 * time.Second,
		Transport: transport,
	}

	return client, func() { transport.CloseIdleConnections(); conns.Wait() }
}

type connGroup struct {
	dial func(context.Context, string, string) (net.Conn, error)
	sync.WaitGroup
}

func (g *connGroup) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	c, err := g.dial(ctx, network, address)
	if err != nil {
		return nil, err
	}
	g.Add(1)
	return &groupConn{Conn: c, group: g}, nil
}

type groupConn struct {
	net.Conn
	group *connGroup
	once  sync.Once
}

func (c *groupConn) Close() error {
	defer c.once.Do(c.group.Done)
	return c.Conn.Close()
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

			c := &Client{Addr: TCP("localhost:9092")}
			testFunc(t, ctx, c)
		})
	}
}

func testConsumerGroupFetchOffsets(t *testing.T, ctx context.Context, c *Client) {
	const totalMessages = 144
	const partitions = 12
	const msgPerPartition = totalMessages / partitions
	topic := makeTopic()
	groupId := makeGroupID()
	createTopic(t, topic, partitions)
	brokers := []string{"localhost:9092"}

	writer := NewWriter(WriterConfig{
		Brokers:   brokers,
		Topic:     topic,
		Dialer:    DefaultDialer,
		Balancer:  &RoundRobin{},
		BatchSize: 1,
	})
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
			t.Errorf("error fetching message: %s", err)
		}
		r.CommitMessages(context.Background(), m)
	}

	offsets, err := c.ConsumerOffsets(ctx, TopicAndGroup{GroupId: groupId, Topic: topic})
	if err != nil {
		t.Fatal(err)
	}

	if len(offsets) != partitions {
		t.Fatalf("expected %d partitions but only received offsets for %d", partitions, len(offsets))
	}

	for i := 0; i < partitions; i++ {
		committedOffset := offsets[i]
		if committedOffset != msgPerPartition {
			t.Fatalf("expected committed offset of %d but received %d", msgPerPartition, committedOffset)
		}
	}
}
