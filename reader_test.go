package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReader(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T, context.Context, *Reader)
	}{
		{
			scenario: "calling Read with a context that has been canceled returns an error",
			function: testReaderReadCanceled,
		},

		{
			scenario: "all messages of the stream are returned when calling ReadMessage repeatedly",
			function: testReaderReadMessages,
		},

		{
			scenario: "test special offsets -1 and -2",
			function: testReaderSetSpecialOffsets,
		},

		{
			scenario: "setting the offset to random values returns the expected messages when Read is called",
			function: testReaderSetRandomOffset,
		},

		{
			scenario: "setting the offset by TimeStamp",
			function: testReaderSetOffsetAt,
		},

		{
			scenario: "calling Lag returns the lag of the last message read from kafka",
			function: testReaderLag,
		},

		{
			scenario: "calling ReadLag returns the current lag of a reader",
			function: testReaderReadLag,
		},

		{ // https://github.com/segmentio/kafka-go/issues/30
			scenario: "reading from an out-of-range offset waits until the context is cancelled",
			function: testReaderOutOfRangeGetsCanceled,
		},

		{
			scenario: "topic being recreated will return an error",
			function: testReaderTopicRecreated,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			r := NewReader(ReaderConfig{
				Brokers:  []string{"localhost:9092"},
				Topic:    makeTopic(),
				MinBytes: 1,
				MaxBytes: 10e6,
				MaxWait:  100 * time.Millisecond,
				Logger:   newTestKafkaLogger(t, ""),
			})
			defer r.Close()
			testFunc(t, ctx, r)
		})
	}
}

func testReaderReadCanceled(t *testing.T, ctx context.Context, r *Reader) {
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	if _, err := r.ReadMessage(ctx); !errors.Is(err, context.Canceled) {
		t.Error(err)
	}
}

func testReaderReadMessages(t *testing.T, ctx context.Context, r *Reader) {
	const N = 1000
	prepareReader(t, ctx, r, makeTestSequence(N)...)

	var offset int64

	for i := 0; i != N; i++ {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Error("reading message at offset", offset, "failed:", err)
			return
		}
		offset = m.Offset + 1
		v, _ := strconv.Atoi(string(m.Value))
		if v != i {
			t.Error("message at index", i, "has wrong value:", v)
			return
		}
	}
}

func testReaderSetSpecialOffsets(t *testing.T, ctx context.Context, r *Reader) {
	prepareReader(t, ctx, r, Message{Value: []byte("first")})
	prepareReader(t, ctx, r, makeTestSequence(3)...)

	go func() {
		time.Sleep(1 * time.Second)
		prepareReader(t, ctx, r, Message{Value: []byte("last")})
	}()

	for _, test := range []struct {
		off, final int64
		want       string
	}{
		{FirstOffset, 1, "first"},
		{LastOffset, 5, "last"},
	} {
		offset := test.off
		if err := r.SetOffset(offset); err != nil {
			t.Error("setting offset", offset, "failed:", err)
		}
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Error("reading at offset", offset, "failed:", err)
		}
		if string(m.Value) != test.want {
			t.Error("message at offset", offset, "has wrong value:", string(m.Value))
		}
		if off := r.Offset(); off != test.final {
			t.Errorf("bad final offset: got %d, want %d", off, test.final)
		}
	}
}

func testReaderSetRandomOffset(t *testing.T, ctx context.Context, r *Reader) {
	const N = 10
	prepareReader(t, ctx, r, makeTestSequence(N)...)

	for i := 0; i != 2*N; i++ {
		offset := rand.Intn(N)
		r.SetOffset(int64(offset))
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Error("seeking to offset", offset, "failed:", err)
			return
		}
		v, _ := strconv.Atoi(string(m.Value))
		if v != offset {
			t.Error("message at offset", offset, "has wrong value:", v)
			return
		}
	}
}

func testReaderSetOffsetAt(t *testing.T, ctx context.Context, r *Reader) {
	// We make 2 batches of messages here with a brief 2 second pause
	// to ensure messages 0...9 will be written a few seconds before messages 10...19
	// We'll then fetch the timestamp for message offset 10 and use that timestamp to set
	// our reader
	const N = 10
	prepareReader(t, ctx, r, makeTestSequence(N)...)
	time.Sleep(time.Second * 2)
	prepareReader(t, ctx, r, makeTestSequence(N)...)

	var ts time.Time
	for i := 0; i < N*2; i++ {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Error("error reading message", err)
		}
		// grab the time for the 10th message
		if i == 10 {
			ts = m.Time
		}
	}

	err := r.SetOffsetAt(ctx, ts)
	if err != nil {
		t.Fatal("error setting offset by timestamp", err)
	}

	m, err := r.ReadMessage(context.Background())
	if err != nil {
		t.Fatal("error reading message", err)
	}

	if m.Offset != 10 {
		t.Errorf("expected offset of 10, received offset %d", m.Offset)
	}
}

func testReaderLag(t *testing.T, ctx context.Context, r *Reader) {
	const N = 5
	prepareReader(t, ctx, r, makeTestSequence(N)...)

	if lag := r.Lag(); lag != 0 {
		t.Errorf("the initial lag value is %d but was expected to be 0", lag)
	}

	for i := 0; i != N; i++ {
		r.ReadMessage(ctx)
		expect := int64(N - (i + 1))

		if lag := r.Lag(); lag != expect {
			t.Errorf("the lag value at offset %d is %d but was expected to be %d", i, lag, expect)
		}
	}
}

func testReaderReadLag(t *testing.T, ctx context.Context, r *Reader) {
	const N = 5
	prepareReader(t, ctx, r, makeTestSequence(N)...)

	if lag, err := r.ReadLag(ctx); err != nil {
		t.Error(err)
	} else if lag != N {
		t.Errorf("the initial lag value is %d but was expected to be %d", lag, N)
	}

	for i := 0; i != N; i++ {
		r.ReadMessage(ctx)
		expect := int64(N - (i + 1))

		if lag, err := r.ReadLag(ctx); err != nil {
			t.Error(err)
		} else if lag != expect {
			t.Errorf("the lag value at offset %d is %d but was expected to be %d", i, lag, expect)
		}
	}
}

func testReaderOutOfRangeGetsCanceled(t *testing.T, ctx context.Context, r *Reader) {
	prepareReader(t, ctx, r, makeTestSequence(10)...)

	const D = 100 * time.Millisecond
	t0 := time.Now()

	ctx, cancel := context.WithTimeout(ctx, D)
	defer cancel()

	if err := r.SetOffset(42); err != nil {
		t.Error(err)
	}

	_, err := r.ReadMessage(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Error("bad error:", err)
	}

	t1 := time.Now()

	if d := t1.Sub(t0); d < D {
		t.Error("ReadMessage returned too early after", d)
	}
}

func createTopic(t *testing.T, topic string, partitions int) {
	t.Helper()

	t.Logf("createTopic(%s, %d)", topic, partitions)

	conn, err := Dial("tcp", "localhost:9092")
	if err != nil {
		err = fmt.Errorf("createTopic, Dial: %w", err)
		t.Fatal(err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		err = fmt.Errorf("createTopic, conn.Controller: %w", err)
		t.Fatal(err)
	}

	conn, err = Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatal(err)
	}

	conn.SetDeadline(time.Now().Add(10 * time.Second))

	_, err = conn.createTopics(createTopicsRequestV0{
		Topics: []createTopicsRequestV0Topic{
			{
				Topic:             topic,
				NumPartitions:     int32(partitions),
				ReplicationFactor: 1,
			},
		},
		Timeout: milliseconds(5 * time.Second),
	})
	if err != nil {
		if !errors.Is(err, TopicAlreadyExists) {
			err = fmt.Errorf("createTopic, conn.createTopics: %w", err)
			t.Error(err)
			t.FailNow()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	waitForTopic(ctx, t, topic)
}

// Block until topic exists.
func waitForTopic(ctx context.Context, t *testing.T, topic string) {
	t.Helper()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("reached deadline before verifying topic existence")
		default:
		}

		cli := &Client{
			Addr:    TCP("localhost:9092"),
			Timeout: 5 * time.Second,
		}

		response, err := cli.Metadata(ctx, &MetadataRequest{
			Addr:   cli.Addr,
			Topics: []string{topic},
		})
		if err != nil {
			t.Fatalf("waitForTopic: error listing topics: %s", err.Error())
		}

		// Find a topic which has at least 1 partition in the metadata response
		for _, top := range response.Topics {
			if top.Name != topic {
				continue
			}

			numPartitions := len(top.Partitions)
			t.Logf("waitForTopic: found topic %q with %d partitions",
				topic, numPartitions)

			if numPartitions > 0 {
				return
			}
		}

		t.Logf("retrying after 100ms")
		time.Sleep(100 * time.Millisecond)
		continue
	}
}

func deleteTopic(t *testing.T, topic ...string) {
	t.Helper()
	conn, err := Dial("tcp", "localhost:9092")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatal(err)
	}

	conn, err = Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatal(err)
	}

	conn.SetDeadline(time.Now().Add(10 * time.Second))

	if err := conn.DeleteTopics(topic...); err != nil {
		t.Fatal(err)
	}
}

func TestReaderOnNonZeroPartition(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T, context.Context, *Reader)
	}{
		{
			scenario: "topic and partition should now be included in header",
			function: testReaderSetsTopicAndPartition,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			topic := makeTopic()
			createTopic(t, topic, 2)
			defer deleteTopic(t, topic)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			r := NewReader(ReaderConfig{
				Brokers:   []string{"localhost:9092"},
				Topic:     topic,
				Partition: 1,
				MinBytes:  1,
				MaxBytes:  10e6,
				MaxWait:   100 * time.Millisecond,
			})
			defer r.Close()
			testFunc(t, ctx, r)
		})
	}
}

func testReaderSetsTopicAndPartition(t *testing.T, ctx context.Context, r *Reader) {
	const N = 3
	prepareReader(t, ctx, r, makeTestSequence(N)...)

	for i := 0; i != N; i++ {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Error("reading message failed:", err)
			return
		}

		if m.Topic == "" {
			t.Error("expected topic to be set")
			return
		}
		if m.Topic != r.config.Topic {
			t.Errorf("expected message to contain topic, %v; got %v", r.config.Topic, m.Topic)
			return
		}
		if m.Partition != r.config.Partition {
			t.Errorf("expected partition to be set; expected 1, got %v", m.Partition)
			return
		}
	}
}

// TestReadTruncatedMessages uses a configuration designed to get the Broker to
// return truncated messages.  It exercises the case where an earlier bug caused
// reading to time out by attempting to read beyond the current response.  This
// test is not perfect, but it is pretty reliable about reproducing the issue.
//
// NOTE : it currently only succeeds against kafka 0.10.1.0, so it will be
// skipped.  It's here so that it can be manually run.
func TestReadTruncatedMessages(t *testing.T) {
	// todo : it would be great to get it to work against 0.11.0.0 so we could
	//        include it in CI unit tests.
	t.Skip()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r := NewReader(ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    makeTopic(),
		MinBytes: 1,
		MaxBytes: 100,
		MaxWait:  100 * time.Millisecond,
	})
	defer r.Close()
	n := 500
	prepareReader(t, ctx, r, makeTestSequence(n)...)
	for i := 0; i < n; i++ {
		if _, err := r.ReadMessage(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func makeTestSequence(n int) []Message {
	base := time.Now()
	msgs := make([]Message, n)
	for i := 0; i != n; i++ {
		msgs[i] = Message{
			Time:  base.Add(time.Duration(i) * time.Millisecond).Truncate(time.Millisecond),
			Value: []byte(strconv.Itoa(i)),
		}
	}
	return msgs
}

func prepareReader(t *testing.T, ctx context.Context, r *Reader, msgs ...Message) {
	config := r.Config()
	var conn *Conn
	var err error

	for {
		if conn, err = DialLeader(ctx, "tcp", "localhost:9092", config.Topic, config.Partition); err == nil {
			break
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	defer conn.Close()

	if _, err := conn.WriteMessages(msgs...); err != nil {
		t.Fatal(err)
	}
}

var (
	benchmarkReaderOnce    sync.Once
	benchmarkReaderTopic   = makeTopic()
	benchmarkReaderPayload = make([]byte, 2*1024)
)

func BenchmarkReader(b *testing.B) {
	const broker = "localhost:9092"
	ctx := context.Background()

	benchmarkReaderOnce.Do(func() {
		conn, err := DialLeader(ctx, "tcp", broker, benchmarkReaderTopic, 0)
		if err != nil {
			b.Fatal(err)
		}
		defer conn.Close()

		msgs := make([]Message, 1000)
		for i := range msgs {
			msgs[i].Value = benchmarkReaderPayload
		}

		for i := 0; i != 10; i++ { // put 10K messages
			if _, err := conn.WriteMessages(msgs...); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()
	})

	r := NewReader(ReaderConfig{
		Brokers:   []string{broker},
		Topic:     benchmarkReaderTopic,
		Partition: 0,
		MinBytes:  1e3,
		MaxBytes:  1e6,
		MaxWait:   100 * time.Millisecond,
	})

	for i := 0; i < b.N; i++ {
		if (i % 10000) == 0 {
			r.SetOffset(-1)
		}
		_, err := r.ReadMessage(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}

	r.Close()
	b.SetBytes(int64(len(benchmarkReaderPayload)))
}

func TestCloseLeavesGroup(t *testing.T) {
	if os.Getenv("KAFKA_VERSION") == "2.3.1" {
		// There's a bug in 2.3.1 that causes the MemberMetadata to be in the wrong format and thus
		// leads to an error when decoding the DescribeGroupsResponse.
		//
		// See https://issues.apache.org/jira/browse/KAFKA-9150 for details.
		t.Skip("Skipping because kafka version is 2.3.1")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	groupID := makeGroupID()
	r := NewReader(ReaderConfig{
		Brokers:          []string{"localhost:9092"},
		Topic:            topic,
		GroupID:          groupID,
		MinBytes:         1,
		MaxBytes:         10e6,
		MaxWait:          100 * time.Millisecond,
		RebalanceTimeout: time.Second,
	})
	prepareReader(t, ctx, r, Message{Value: []byte("test")})

	conn, err := Dial("tcp", r.config.Brokers[0])
	if err != nil {
		t.Fatalf("error dialing: %v", err)
	}
	defer conn.Close()

	client, shutdown := newLocalClient()
	defer shutdown()

	descGroups := func() DescribeGroupsResponse {
		resp, err := client.DescribeGroups(
			ctx,
			&DescribeGroupsRequest{
				GroupIDs: []string{groupID},
			},
		)
		if err != nil {
			t.Fatalf("error from describeGroups %v", err)
		}
		return *resp
	}

	_, err = r.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("our reader never joind its group or couldn't read a message: %v", err)
	}
	resp := descGroups()
	if len(resp.Groups) != 1 {
		t.Fatalf("expected 1 group. got: %d", len(resp.Groups))
	}
	if len(resp.Groups[0].Members) != 1 {
		t.Fatalf("expected group membership size of %d, but got %d", 1, len(resp.Groups[0].Members))
	}

	err = r.Close()
	if err != nil {
		t.Fatalf("unexpected error closing reader: %s", err.Error())
	}
	resp = descGroups()
	if len(resp.Groups) != 1 {
		t.Fatalf("expected 1 group. got: %d", len(resp.Groups))
	}
	if len(resp.Groups[0].Members) != 0 {
		t.Fatalf("expected group membership size of %d, but got %d", 0, len(resp.Groups[0].Members))
	}
}

func testConsumerGroupImmediateClose(t *testing.T, ctx context.Context, r *Reader) {
	if err := r.Close(); err != nil {
		t.Fatalf("bad err: %v", err)
	}
}

func testConsumerGroupSimple(t *testing.T, ctx context.Context, r *Reader) {
	if err := r.Close(); err != nil {
		t.Fatalf("bad err: %v", err)
	}
}

func TestReaderSetOffsetWhenConsumerGroupsEnabled(t *testing.T) {
	r := &Reader{config: ReaderConfig{GroupID: "not-zero"}}
	if err := r.SetOffset(LastOffset); !errors.Is(err, errNotAvailableWithGroup) {
		t.Fatalf("expected %v; got %v", errNotAvailableWithGroup, err)
	}
}

func TestReaderOffsetWhenConsumerGroupsEnabled(t *testing.T) {
	r := &Reader{config: ReaderConfig{GroupID: "not-zero"}}
	if offset := r.Offset(); offset != -1 {
		t.Fatalf("expected -1; got %v", offset)
	}
}

func TestReaderLagWhenConsumerGroupsEnabled(t *testing.T) {
	r := &Reader{config: ReaderConfig{GroupID: "not-zero"}}
	if offset := r.Lag(); offset != -1 {
		t.Fatalf("expected -1; got %v", offset)
	}
}

func TestReaderReadLagReturnsZeroLagWhenConsumerGroupsEnabled(t *testing.T) {
	r := &Reader{config: ReaderConfig{GroupID: "not-zero"}}
	lag, err := r.ReadLag(context.Background())

	if !errors.Is(err, errNotAvailableWithGroup) {
		t.Fatalf("expected %v; got %v", errNotAvailableWithGroup, err)
	}

	if lag != 0 {
		t.Fatalf("expected 0; got %d", lag)
	}
}

func TestReaderPartitionWhenConsumerGroupsEnabled(t *testing.T) {
	invoke := func() (boom bool) {
		defer func() {
			if r := recover(); r != nil {
				boom = true
			}
		}()

		NewReader(ReaderConfig{
			GroupID:   "set",
			Partition: 1,
		})
		return false
	}

	if !invoke() {
		t.Fatalf("expected panic; but NewReader worked?!")
	}
}

func TestExtractTopics(t *testing.T) {
	testCases := map[string]struct {
		Members []GroupMember
		Topics  []string
	}{
		"nil": {},
		"single member, single topic": {
			Members: []GroupMember{
				{
					ID:     "a",
					Topics: []string{"topic"},
				},
			},
			Topics: []string{"topic"},
		},
		"two members, single topic": {
			Members: []GroupMember{
				{
					ID:     "a",
					Topics: []string{"topic"},
				},
				{
					ID:     "b",
					Topics: []string{"topic"},
				},
			},
			Topics: []string{"topic"},
		},
		"two members, two topics": {
			Members: []GroupMember{
				{
					ID:     "a",
					Topics: []string{"topic-1"},
				},
				{
					ID:     "b",
					Topics: []string{"topic-2"},
				},
			},
			Topics: []string{"topic-1", "topic-2"},
		},
		"three members, three shared topics": {
			Members: []GroupMember{
				{
					ID:     "a",
					Topics: []string{"topic-1", "topic-2"},
				},
				{
					ID:     "b",
					Topics: []string{"topic-2", "topic-3"},
				},
				{
					ID:     "c",
					Topics: []string{"topic-3", "topic-1"},
				},
			},
			Topics: []string{"topic-1", "topic-2", "topic-3"},
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			topics := extractTopics(tc.Members)
			if !reflect.DeepEqual(tc.Topics, topics) {
				t.Errorf("expected %v; got %v", tc.Topics, topics)
			}
		})
	}
}

func TestReaderConsumerGroup(t *testing.T) {
	tests := []struct {
		scenario       string
		partitions     int
		commitInterval time.Duration
		function       func(*testing.T, context.Context, *Reader)
	}{
		{
			scenario:   "basic handshake",
			partitions: 1,
			function:   testReaderConsumerGroupHandshake,
		},
		{
			scenario:   "verify offset committed",
			partitions: 1,
			function:   testReaderConsumerGroupVerifyOffsetCommitted,
		},

		{
			scenario:       "verify offset committed when using interval committer",
			partitions:     1,
			commitInterval: 400 * time.Millisecond,
			function:       testReaderConsumerGroupVerifyPeriodicOffsetCommitter,
		},

		{
			scenario:   "rebalance across many partitions and consumers",
			partitions: 8,
			function:   testReaderConsumerGroupRebalanceAcrossManyPartitionsAndConsumers,
		},

		{
			scenario:   "consumer group commits on close",
			partitions: 3,
			function:   testReaderConsumerGroupVerifyCommitsOnClose,
		},

		{
			scenario:   "consumer group rebalance",
			partitions: 3,
			function:   testReaderConsumerGroupRebalance,
		},

		{
			scenario:   "consumer group rebalance across topics",
			partitions: 3,
			function:   testReaderConsumerGroupRebalanceAcrossTopics,
		},

		{
			scenario:   "consumer group reads content across partitions",
			partitions: 3,
			function:   testReaderConsumerGroupReadContentAcrossPartitions,
		},

		{
			scenario:   "Close immediately after NewReader",
			partitions: 1,
			function:   testConsumerGroupImmediateClose,
		},

		{
			scenario:   "Close immediately after NewReader",
			partitions: 1,
			function:   testConsumerGroupSimple,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			// It appears that some of the tests depend on all these tests being
			// run concurrently to pass... this is brittle and should be fixed
			// at some point.
			t.Parallel()

			topic := makeTopic()
			createTopic(t, topic, test.partitions)
			defer deleteTopic(t, topic)

			groupID := makeGroupID()
			r := NewReader(ReaderConfig{
				Brokers:           []string{"localhost:9092"},
				Topic:             topic,
				GroupID:           groupID,
				HeartbeatInterval: 2 * time.Second,
				CommitInterval:    test.commitInterval,
				RebalanceTimeout:  2 * time.Second,
				RetentionTime:     time.Hour,
				MinBytes:          1,
				MaxBytes:          1e6,
			})
			defer r.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			test.function(t, ctx, r)
		})
	}
}

func testReaderConsumerGroupHandshake(t *testing.T, ctx context.Context, r *Reader) {
	prepareReader(t, context.Background(), r, makeTestSequence(5)...)

	m, err := r.ReadMessage(ctx)
	if err != nil {
		t.Errorf("bad err: %v", err)
	}
	if m.Topic != r.config.Topic {
		t.Errorf("topic not set")
	}
	if m.Offset != 0 {
		t.Errorf("offset not set")
	}

	m, err = r.ReadMessage(ctx)
	if err != nil {
		t.Errorf("bad err: %v", err)
	}
	if m.Topic != r.config.Topic {
		t.Errorf("topic not set")
	}
	if m.Offset != 1 {
		t.Errorf("offset not set")
	}
}

func testReaderConsumerGroupVerifyOffsetCommitted(t *testing.T, ctx context.Context, r *Reader) {
	prepareReader(t, context.Background(), r, makeTestSequence(3)...)

	if _, err := r.FetchMessage(ctx); err != nil {
		t.Errorf("bad err: %v", err) // skip the first message
	}

	m, err := r.FetchMessage(ctx)
	if err != nil {
		t.Errorf("bad err: %v", err)
	}

	if err := r.CommitMessages(ctx, m); err != nil {
		t.Errorf("bad commit message: %v", err)
	}

	offsets := getOffsets(t, r.config)
	if expected := map[int]int64{0: m.Offset + 1}; !reflect.DeepEqual(expected, offsets) {
		t.Errorf("expected %v; got %v", expected, offsets)
	}
}

func testReaderConsumerGroupVerifyPeriodicOffsetCommitter(t *testing.T, ctx context.Context, r *Reader) {
	prepareReader(t, context.Background(), r, makeTestSequence(3)...)

	if _, err := r.FetchMessage(ctx); err != nil {
		t.Errorf("bad err: %v", err) // skip the first message
	}

	m, err := r.FetchMessage(ctx)
	if err != nil {
		t.Errorf("bad err: %v", err)
	}

	started := time.Now()
	if err := r.CommitMessages(ctx, m); err != nil {
		t.Errorf("bad commit message: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Millisecond {
		t.Errorf("background commits should happen nearly instantly")
	}

	// wait for committer to pick up the commits
	time.Sleep(r.config.CommitInterval * 3)

	offsets := getOffsets(t, r.config)
	if expected := map[int]int64{0: m.Offset + 1}; !reflect.DeepEqual(expected, offsets) {
		t.Errorf("expected %v; got %v", expected, offsets)
	}
}

func testReaderConsumerGroupVerifyCommitsOnClose(t *testing.T, ctx context.Context, r *Reader) {
	prepareReader(t, context.Background(), r, makeTestSequence(3)...)

	if _, err := r.FetchMessage(ctx); err != nil {
		t.Errorf("bad err: %v", err) // skip the first message
	}

	m, err := r.FetchMessage(ctx)
	if err != nil {
		t.Errorf("bad err: %v", err)
	}

	if err := r.CommitMessages(ctx, m); err != nil {
		t.Errorf("bad commit message: %v", err)
	}

	if err := r.Close(); err != nil {
		t.Errorf("bad Close: %v", err)
	}

	r2 := NewReader(r.config)
	defer r2.Close()

	offsets := getOffsets(t, r2.config)
	if expected := map[int]int64{0: m.Offset + 1}; !reflect.DeepEqual(expected, offsets) {
		t.Errorf("expected %v; got %v", expected, offsets)
	}
}

func testReaderConsumerGroupReadContentAcrossPartitions(t *testing.T, ctx context.Context, r *Reader) {
	const N = 12

	client, shutdown := newLocalClient()
	defer shutdown()

	writer := &Writer{
		Addr:      TCP(r.config.Brokers...),
		Topic:     r.config.Topic,
		Balancer:  &RoundRobin{},
		BatchSize: 1,
		Transport: client.Transport,
	}
	if err := writer.WriteMessages(ctx, makeTestSequence(N)...); err != nil {
		t.Fatalf("bad write messages: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("bad write err: %v", err)
	}

	partitions := map[int]struct{}{}
	for i := 0; i < N; i++ {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			t.Errorf("bad error: %s", err)
		}
		partitions[m.Partition] = struct{}{}
	}

	if v := len(partitions); v != 3 {
		t.Errorf("expected messages across 3 partitions; got messages across %v partitions", v)
	}
}

func testReaderConsumerGroupRebalance(t *testing.T, ctx context.Context, r *Reader) {
	r2 := NewReader(r.config)
	defer r.Close()

	const (
		N          = 12
		partitions = 2
	)

	client, shutdown := newLocalClient()
	defer shutdown()

	// rebalance should result in 12 message in each of the partitions
	writer := &Writer{
		Addr:      TCP(r.config.Brokers...),
		Topic:     r.config.Topic,
		Balancer:  &RoundRobin{},
		BatchSize: 1,
		Transport: client.Transport,
	}
	if err := writer.WriteMessages(ctx, makeTestSequence(N*partitions)...); err != nil {
		t.Fatalf("bad write messages: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("bad write err: %v", err)
	}

	// after rebalance, each reader should have a partition to itself
	for i := 0; i < N; i++ {
		if _, err := r2.FetchMessage(ctx); err != nil {
			t.Errorf("expect to read from reader 2")
		}
		if _, err := r.FetchMessage(ctx); err != nil {
			t.Errorf("expect to read from reader 1")
		}
	}
}

func testReaderConsumerGroupRebalanceAcrossTopics(t *testing.T, ctx context.Context, r *Reader) {
	// create a second reader that shares the groupID, but reads from a different topic
	client, topic2, shutdown := newLocalClientAndTopic()
	defer shutdown()

	r2 := NewReader(ReaderConfig{
		Brokers:           r.config.Brokers,
		Topic:             topic2,
		GroupID:           r.config.GroupID,
		HeartbeatInterval: r.config.HeartbeatInterval,
		SessionTimeout:    r.config.SessionTimeout,
		RetentionTime:     r.config.RetentionTime,
		MinBytes:          r.config.MinBytes,
		MaxBytes:          r.config.MaxBytes,
		Logger:            r.config.Logger,
	})
	defer r.Close()
	prepareReader(t, ctx, r2, makeTestSequence(1)...)

	const (
		N = 12
	)

	// write messages across both partitions
	writer := &Writer{
		Addr:      TCP(r.config.Brokers...),
		Topic:     r.config.Topic,
		Balancer:  &RoundRobin{},
		BatchSize: 1,
		Transport: client.Transport,
	}
	if err := writer.WriteMessages(ctx, makeTestSequence(N)...); err != nil {
		t.Fatalf("bad write messages: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("bad write err: %v", err)
	}

	// after rebalance, r2 should read topic2 and r1 should read ALL of the original topic
	if _, err := r2.FetchMessage(ctx); err != nil {
		t.Errorf("expect to read from reader 2")
	}

	// all N messages on the original topic should be read by the original reader
	for i := 0; i < N; i++ {
		if _, err := r.FetchMessage(ctx); err != nil {
			t.Errorf("expect to read from reader 1")
		}
	}
}

func testReaderConsumerGroupRebalanceAcrossManyPartitionsAndConsumers(t *testing.T, ctx context.Context, r *Reader) {
	// I've rebalanced up to 100 servers, but the rebalance can take upwards
	// of a minute and that seems too long for unit tests.  Also, setting this
	// to a larger number seems to make the kafka broker unresponsive.
	// TODO research if there's a way to reduce rebalance time across many partitions
	// svls: the described behavior is due to the thundering herd of readers
	//       hitting the rebalance timeout.  introducing the 100ms sleep in the
	//       loop below in order to give time for the sync group to finish has
	//       greatly helped, though we still hit the timeout from time to time.
	const N = 8

	var readers []*Reader

	for i := 0; i < N-1; i++ {
		reader := NewReader(r.config)
		readers = append(readers, reader)
		time.Sleep(100 * time.Millisecond)
	}
	defer func() {
		for _, r := range readers {
			r.Close()
			time.Sleep(100 * time.Millisecond)
		}
	}()

	client, shutdown := newLocalClient()
	defer shutdown()

	// write messages across both partitions
	writer := &Writer{
		Addr:      TCP(r.config.Brokers...),
		Topic:     r.config.Topic,
		Balancer:  &RoundRobin{},
		BatchSize: 1,
		Transport: client.Transport,
	}
	if err := writer.WriteMessages(ctx, makeTestSequence(N*3)...); err != nil {
		t.Fatalf("bad write messages: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("bad write err: %v", err)
	}

	// all N messages on the original topic should be read by the original reader
	for i := 0; i < N-1; i++ {
		if _, err := readers[i].FetchMessage(ctx); err != nil {
			t.Errorf("reader %v expected to read 1 message", i)
		}
	}

	if _, err := r.FetchMessage(ctx); err != nil {
		t.Errorf("expect to read from original reader")
	}
}

func TestOffsetStash(t *testing.T) {
	const topic = "topic"

	newMessage := func(partition int, offset int64) Message {
		return Message{
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
		}
	}

	tests := map[string]struct {
		Given    offsetStash
		Messages []Message
		Expected offsetStash
	}{
		"nil": {},
		"empty given, single message": {
			Given:    offsetStash{},
			Messages: []Message{newMessage(0, 0)},
			Expected: offsetStash{
				topic: {0: 1},
			},
		},
		"ignores earlier offsets": {
			Given: offsetStash{
				topic: {0: 2},
			},
			Messages: []Message{newMessage(0, 0)},
			Expected: offsetStash{
				topic: {0: 2},
			},
		},
		"uses latest offset": {
			Given: offsetStash{},
			Messages: []Message{
				newMessage(0, 2),
				newMessage(0, 3),
				newMessage(0, 1),
			},
			Expected: offsetStash{
				topic: {0: 4},
			},
		},
		"uses latest offset, across multiple topics": {
			Given: offsetStash{},
			Messages: []Message{
				newMessage(0, 2),
				newMessage(0, 3),
				newMessage(0, 1),
				newMessage(1, 5),
				newMessage(1, 6),
			},
			Expected: offsetStash{
				topic: {
					0: 4,
					1: 7,
				},
			},
		},
	}

	for label, test := range tests {
		t.Run(label, func(t *testing.T) {
			test.Given.merge(makeCommits(test.Messages...))
			if !reflect.DeepEqual(test.Expected, test.Given) {
				t.Errorf("expected %v; got %v", test.Expected, test.Given)
			}
		})
	}
}

func TestValidateReader(t *testing.T) {
	tests := []struct {
		config       ReaderConfig
		errorOccured bool
	}{
		{config: ReaderConfig{}, errorOccured: true},
		{config: ReaderConfig{Brokers: []string{"broker1"}}, errorOccured: true},
		{config: ReaderConfig{Brokers: []string{"broker1"}, Topic: "topic1"}, errorOccured: false},
		{config: ReaderConfig{Brokers: []string{"broker1"}, Topic: "topic1", Partition: -1}, errorOccured: true},
		{config: ReaderConfig{Brokers: []string{"broker1"}, Topic: "topic1", Partition: 1, MinBytes: -1}, errorOccured: true},
		{config: ReaderConfig{Brokers: []string{"broker1"}, Topic: "topic1", Partition: 1, MinBytes: 5, MaxBytes: -1}, errorOccured: true},
		{config: ReaderConfig{Brokers: []string{"broker1"}, Topic: "topic1", Partition: 1, MinBytes: 5, MaxBytes: 6}, errorOccured: false},
	}
	for _, test := range tests {
		err := test.config.Validate()
		if test.errorOccured && err == nil {
			t.Fail()
		}
		if !test.errorOccured && err != nil {
			t.Fail()
		}
	}
}

func TestCommitLoopImmediateFlushOnGenerationEnd(t *testing.T) {
	t.Parallel()
	var committedOffset int64
	var commitCount int
	gen := &Generation{
		conn: mockCoordinator{
			offsetCommitFunc: func(r offsetCommitRequestV2) (offsetCommitResponseV2, error) {
				commitCount++
				committedOffset = r.Topics[0].Partitions[0].Offset
				return offsetCommitResponseV2{}, nil
			},
		},
		done:     make(chan struct{}),
		log:      func(func(Logger)) {},
		logError: func(func(Logger)) {},
		joined:   make(chan struct{}),
	}

	// initialize commits so that the commitLoopImmediate select statement blocks
	r := &Reader{stctx: context.Background(), commits: make(chan commitRequest, 100)}

	for i := 0; i < 100; i++ {
		cr := commitRequest{
			commits: []commit{{
				topic:     "topic",
				partition: 0,
				offset:    int64(i) + 1,
			}},
			errch: make(chan<- error, 1),
		}
		r.commits <- cr
	}

	gen.Start(func(ctx context.Context) {
		r.commitLoopImmediate(ctx, gen)
	})

	gen.close()

	if committedOffset != 100 {
		t.Fatalf("expected commited offset to be 100 but got %d", committedOffset)
	}

	if commitCount >= 100 {
		t.Fatalf("expected a single final commit on generation end got %d", commitCount)
	}
}

func TestCommitOffsetsWithRetry(t *testing.T) {
	offsets := offsetStash{"topic": {0: 0}}

	tests := map[string]struct {
		Fails       int
		Invocations int
		HasError    bool
	}{
		"happy path": {
			Invocations: 1,
		},
		"1 retry": {
			Fails:       1,
			Invocations: 2,
		},
		"out of retries": {
			Fails:       defaultCommitRetries + 1,
			Invocations: defaultCommitRetries,
			HasError:    true,
		},
	}

	for label, test := range tests {
		t.Run(label, func(t *testing.T) {
			count := 0
			gen := &Generation{
				conn: mockCoordinator{
					offsetCommitFunc: func(offsetCommitRequestV2) (offsetCommitResponseV2, error) {
						count++
						if count <= test.Fails {
							return offsetCommitResponseV2{}, io.EOF
						}
						return offsetCommitResponseV2{}, nil
					},
				},
				done:     make(chan struct{}),
				log:      func(func(Logger)) {},
				logError: func(func(Logger)) {},
			}

			r := &Reader{stctx: context.Background()}
			err := r.commitOffsetsWithRetry(gen, offsets, defaultCommitRetries)
			switch {
			case test.HasError && err == nil:
				t.Error("bad err: expected not nil; got nil")
			case !test.HasError && err != nil:
				t.Errorf("bad err: expected nil; got %v", err)
			}
		})
	}
}

// Test that a reader won't continually rebalance when there are more consumers
// than partitions in a group.
// https://github.com/segmentio/kafka-go/issues/200
func TestRebalanceTooManyConsumers(t *testing.T) {
	ctx := context.Background()
	conf := ReaderConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: makeGroupID(),
		Topic:   makeTopic(),
		MaxWait: time.Second,
	}

	// Create the first reader and wait for it to become the leader.
	r1 := NewReader(conf)
	prepareReader(t, ctx, r1, makeTestSequence(1)...)
	r1.ReadMessage(ctx)
	// Clear the stats from the first rebalance.
	r1.Stats()

	// Second reader should cause one rebalance for each r1 and r2.
	r2 := NewReader(conf)

	// Wait for rebalances.
	time.Sleep(5 * time.Second)

	// Before the fix, r2 would cause continuous rebalances,
	// as it tried to handshake() repeatedly.
	rebalances := r1.Stats().Rebalances + r2.Stats().Rebalances
	if rebalances > 2 {
		t.Errorf("unexpected rebalances to first reader, got %d", rebalances)
	}
}

func TestConsumerGroupWithMissingTopic(t *testing.T) {
	t.Skip("this test doesn't work when the cluster is configured to auto-create topics")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conf := ReaderConfig{
		Brokers:                []string{"localhost:9092"},
		GroupID:                makeGroupID(),
		Topic:                  makeTopic(),
		MaxWait:                time.Second,
		PartitionWatchInterval: 100 * time.Millisecond,
		WatchPartitionChanges:  true,
	}

	r := NewReader(conf)
	defer r.Close()

	recvErr := make(chan error, 1)
	go func() {
		_, err := r.ReadMessage(ctx)
		recvErr <- err
	}()

	time.Sleep(time.Second)
	client, shutdown := newLocalClientWithTopic(conf.Topic, 1)
	defer shutdown()

	w := &Writer{
		Addr:         TCP(r.config.Brokers...),
		Topic:        r.config.Topic,
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    1,
		Transport:    client.Transport,
	}
	defer w.Close()
	if err := w.WriteMessages(ctx, Message{}); err != nil {
		t.Fatalf("write error: %+v", err)
	}

	if err := <-recvErr; err != nil {
		t.Fatalf("read error: %+v", err)
	}

	nMsgs := r.Stats().Messages
	if nMsgs != 1 {
		t.Fatalf("expected to receive one message, but got %d", nMsgs)
	}
}

func TestConsumerGroupWithTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ReaderConfig{
		Brokers:                []string{"localhost:9092"},
		GroupID:                makeGroupID(),
		Topic:                  makeTopic(),
		MaxWait:                time.Second,
		PartitionWatchInterval: 100 * time.Millisecond,
		WatchPartitionChanges:  true,
		Logger:                 newTestKafkaLogger(t, "Reader:"),
	}

	r := NewReader(conf)
	defer r.Close()

	recvErr := make(chan error, len(conf.GroupTopics))
	go func() {
		msg, err := r.ReadMessage(ctx)
		t.Log(msg)
		recvErr <- err
	}()

	time.Sleep(conf.MaxWait)

	client, shutdown := newLocalClientWithTopic(conf.Topic, 1)
	defer shutdown()

	w := &Writer{
		Addr:         TCP(r.config.Brokers...),
		Topic:        conf.Topic,
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    1,
		Transport:    client.Transport,
		Logger:       newTestKafkaLogger(t, "Writer:"),
	}
	defer w.Close()
	if err := w.WriteMessages(ctx, Message{Value: []byte(conf.Topic)}); err != nil {
		t.Fatalf("write error: %+v", err)
	}

	if err := <-recvErr; err != nil {
		t.Fatalf("read error: %+v", err)
	}

	nMsgs := r.Stats().Messages
	if nMsgs != 1 {
		t.Fatalf("expected to receive 1 message, but got %d", nMsgs)
	}
}

func TestConsumerGroupWithGroupTopicsSingle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ReaderConfig{
		Brokers:                []string{"localhost:9092"},
		GroupID:                makeGroupID(),
		GroupTopics:            []string{makeTopic()},
		MaxWait:                time.Second,
		PartitionWatchInterval: 100 * time.Millisecond,
		WatchPartitionChanges:  true,
		Logger:                 newTestKafkaLogger(t, "Reader:"),
	}

	r := NewReader(conf)
	defer r.Close()

	recvErr := make(chan error, len(conf.GroupTopics))
	go func() {
		msg, err := r.ReadMessage(ctx)
		t.Log(msg)
		recvErr <- err
	}()

	time.Sleep(conf.MaxWait)

	for i, topic := range conf.GroupTopics {
		client, shutdown := newLocalClientWithTopic(topic, 1)
		defer shutdown()

		w := &Writer{
			Addr:         TCP(r.config.Brokers...),
			Topic:        topic,
			BatchTimeout: 10 * time.Millisecond,
			BatchSize:    1,
			Transport:    client.Transport,
			Logger:       newTestKafkaLogger(t, fmt.Sprintf("Writer(%d):", i)),
		}
		defer w.Close()
		if err := w.WriteMessages(ctx, Message{Value: []byte(topic)}); err != nil {
			t.Fatalf("write error: %+v", err)
		}
	}

	if err := <-recvErr; err != nil {
		t.Fatalf("read error: %+v", err)
	}

	nMsgs := r.Stats().Messages
	if nMsgs != int64(len(conf.GroupTopics)) {
		t.Fatalf("expected to receive %d messages, but got %d", len(conf.GroupTopics), nMsgs)
	}
}

func TestConsumerGroupWithGroupTopicsMultiple(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	client, shutdown := newLocalClient()
	defer shutdown()
	t1 := makeTopic()
	createTopic(t, t1, 1)
	defer deleteTopic(t, t1)
	t2 := makeTopic()
	createTopic(t, t2, 1)
	defer deleteTopic(t, t2)
	conf := ReaderConfig{
		Brokers:                []string{"localhost:9092"},
		GroupID:                makeGroupID(),
		GroupTopics:            []string{t1, t2},
		MaxWait:                time.Second,
		PartitionWatchInterval: 100 * time.Millisecond,
		WatchPartitionChanges:  true,
		Logger:                 newTestKafkaLogger(t, "Reader:"),
	}

	r := NewReader(conf)

	w := &Writer{
		Addr:         TCP(r.config.Brokers...),
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    1,
		Transport:    client.Transport,
		Logger:       newTestKafkaLogger(t, "Writer:"),
	}
	defer w.Close()

	time.Sleep(time.Second)

	msgs := make([]Message, 0, len(conf.GroupTopics))
	for _, topic := range conf.GroupTopics {
		msgs = append(msgs, Message{Topic: topic})
	}
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		t.Logf("write error: %+v", err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(len(msgs))

	go func() {
		wg.Wait()
		t.Log("closing reader")
		r.Close()
	}()

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				t.Log("reader closed")
				break
			}

			t.Fatalf("read error: %+v", err)
		} else {
			t.Logf("message read: %+v", msg)
			wg.Done()
		}
	}

	nMsgs := r.Stats().Messages
	if nMsgs != int64(len(conf.GroupTopics)) {
		t.Fatalf("expected to receive %d messages, but got %d", len(conf.GroupTopics), nMsgs)
	}
}

func getOffsets(t *testing.T, config ReaderConfig) map[int]int64 {
	// minimal config required to lookup coordinator
	cg := ConsumerGroup{
		config: ConsumerGroupConfig{
			ID:      config.GroupID,
			Brokers: config.Brokers,
			Dialer:  config.Dialer,
		},
	}

	conn, err := cg.coordinator()
	if err != nil {
		t.Errorf("unable to connect to coordinator: %v", err)
	}
	defer conn.Close()

	offsets, err := conn.offsetFetch(offsetFetchRequestV1{
		GroupID: config.GroupID,
		Topics: []offsetFetchRequestV1Topic{{
			Topic:      config.Topic,
			Partitions: []int32{0},
		}},
	})
	if err != nil {
		t.Errorf("bad fetchOffsets: %v", err)
	}

	m := map[int]int64{}

	for _, r := range offsets.Responses {
		if r.Topic == config.Topic {
			for _, p := range r.PartitionResponses {
				m[int(p.Partition)] = p.Offset
			}
		}
	}

	return m
}

const (
	connTO     = 1 * time.Second
	connTestTO = 2 * connTO
)

func TestErrorCannotConnect(t *testing.T) {
	r := NewReader(ReaderConfig{
		Brokers:     []string{"localhost:9093"},
		Dialer:      &Dialer{Timeout: connTO},
		MaxAttempts: 1,
		Topic:       makeTopic(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), connTestTO)
	defer cancel()

	_, err := r.FetchMessage(ctx)
	if err == nil || ctx.Err() != nil {
		t.Errorf("Reader.FetchMessage must fail when it cannot " +
			"connect")
	}
}

func TestErrorCannotConnectGroupSubscription(t *testing.T) {
	r := NewReader(ReaderConfig{
		Brokers:     []string{"localhost:9093"},
		Dialer:      &Dialer{Timeout: 1 * time.Second},
		GroupID:     "foobar",
		MaxAttempts: 1,
		Topic:       makeTopic(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), connTestTO)
	defer cancel()

	_, err := r.FetchMessage(ctx)
	if err == nil || ctx.Err() != nil {
		t.Errorf("Reader.FetchMessage with a group subscription " +
			"must fail when it cannot connect")
	}
}

// Tests that the reader can handle messages where the response is truncated
// due to reaching MaxBytes.
//
// If MaxBytes is too small to fit 1 record then it will never truncate, so
// we start from a small message size and increase it until we are sure
// truncation has happened at some point.
func TestReaderTruncatedResponse(t *testing.T) {
	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	readerMaxBytes := 100
	batchSize := 4
	maxMsgPadding := 5
	readContextTimeout := 10 * time.Second

	var msgs []Message
	// The key of each message
	n := 0
	// `i` is the amount of padding per message
	for i := 0; i < maxMsgPadding; i++ {
		bb := bytes.Buffer{}
		for x := 0; x < i; x++ {
			_, err := bb.WriteRune('0')
			require.NoError(t, err)
		}
		padding := bb.Bytes()
		// `j` is the number of times the message repeats
		for j := 0; j < batchSize*4; j++ {
			msgs = append(msgs, Message{
				Key:   []byte(fmt.Sprintf("%05d", n)),
				Value: padding,
			})
			n++
		}
	}

	wr := NewWriter(WriterConfig{
		Brokers:   []string{"localhost:9092"},
		BatchSize: batchSize,
		Async:     false,
		Topic:     topic,
		Balancer:  &LeastBytes{},
	})
	err := wr.WriteMessages(context.Background(), msgs...)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), readContextTimeout)
	defer cancel()
	r := NewReader(ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: readerMaxBytes,
		// Speed up testing
		MaxWait: 100 * time.Millisecond,
	})
	defer r.Close()

	expectedKeys := map[string]struct{}{}
	for _, k := range msgs {
		expectedKeys[string(k.Key)] = struct{}{}
	}
	keys := map[string]struct{}{}
	for {
		m, err := r.FetchMessage(ctx)
		require.NoError(t, err)
		keys[string(m.Key)] = struct{}{}

		t.Logf("got key %s have %d keys expect %d\n", string(m.Key), len(keys), len(expectedKeys))
		if len(keys) == len(expectedKeys) {
			require.Equal(t, expectedKeys, keys)
			return
		}
	}
}

// Tests that the reader can read record batches from log compacted topics
// where the batch ends with compacted records.
//
// This test forces varying sized chunks of duplicated messages along with
// configuring the topic with a minimal `segment.bytes` in order to
// guarantee that at least 1 batch can be compacted down to 0 "unread" messages
// with at least 1 "old" message otherwise the batch is skipped entirely.
func TestReaderReadCompactedMessage(t *testing.T) {
	topic := makeTopic()
	createTopicWithCompaction(t, topic, 1)
	defer deleteTopic(t, topic)

	msgs := makeTestDuplicateSequence()

	writeMessagesForCompactionCheck(t, topic, msgs)

	expectedKeys := map[string]int{}
	for _, msg := range msgs {
		expectedKeys[string(msg.Key)] = 1
	}

	// kafka 2.0.1 is extra slow
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
	defer cancel()
	for {
		success := func() bool {
			r := NewReader(ReaderConfig{
				Brokers:  []string{"localhost:9092"},
				Topic:    topic,
				MinBytes: 200,
				MaxBytes: 200,
				// Speed up testing
				MaxWait: 100 * time.Millisecond,
			})
			defer r.Close()

			keys := map[string]int{}
			for {
				m, err := r.FetchMessage(ctx)
				if err != nil {
					t.Logf("can't get message from compacted log: %v", err)
					return false
				}
				keys[string(m.Key)]++

				if len(keys) == countKeys(msgs) {
					t.Logf("got keys: %+v", keys)
					return reflect.DeepEqual(keys, expectedKeys)
				}
			}
		}()
		if success {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		default:
		}
	}
}

func TestReaderClose(t *testing.T) {
	t.Parallel()

	r := NewReader(ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   makeTopic(),
		MaxWait: 2 * time.Second,
	})
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := r.FetchMessage(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("bad err: %v", err)
	}

	t0 := time.Now()
	r.Close()
	if time.Since(t0) > 100*time.Millisecond {
		t.Errorf("r.Close took too long")
	}
}

func BenchmarkReaderClose(b *testing.B) {
	r := NewReader(ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   makeTopic(),
		MaxWait: 2 * time.Second,
	})
	defer r.Close()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err := r.FetchMessage(ctx)
		if errors.Is(err, context.DeadlineExceeded) {
			b.Errorf("bad err: %v", err)
		}
	}
}

// writeMessagesForCompactionCheck writes messages with specific writer configuration.
func writeMessagesForCompactionCheck(t *testing.T, topic string, msgs []Message) {
	t.Helper()

	wr := NewWriter(WriterConfig{
		Brokers: []string{"localhost:9092"},
		// Batch size must be large enough to have multiple compacted records
		// for testing more edge cases.
		BatchSize: 3,
		Async:     false,
		Topic:     topic,
		Balancer:  &LeastBytes{},
	})
	err := wr.WriteMessages(context.Background(), msgs...)
	require.NoError(t, err)
}

// makeTestDuplicateSequence creates messages for compacted log testing
//
// All keys and values are 4 characters long to tightly control how many
// messages are per log segment.
func makeTestDuplicateSequence() []Message {
	var msgs []Message
	// `n` is an increasing counter so it is never compacted.
	n := 0
	// `i` determines how many compacted records to create
	for i := 0; i < 5; i++ {
		// `j` is how many times the current pattern repeats. We repeat because
		// as long as we have a pattern that is slightly larger/smaller than
		// the log segment size then if we repeat enough it will eventually
		// try all configurations.
		for j := 0; j < 30; j++ {
			msgs = append(msgs, Message{
				Key:   []byte(fmt.Sprintf("%04d", n)),
				Value: []byte(fmt.Sprintf("%04d", n)),
			})
			n++

			// This produces the duplicated messages to compact.
			for k := 0; k < i; k++ {
				msgs = append(msgs, Message{
					Key:   []byte("dup_"),
					Value: []byte("dup_"),
				})
			}
		}
	}

	// "end markers" to force duplicate message outside of the last segment of
	// the log so that they can all be compacted.
	for i := 0; i < 10; i++ {
		msgs = append(msgs, Message{
			Key:   []byte(fmt.Sprintf("e-%02d", i)),
			Value: []byte(fmt.Sprintf("e-%02d", i)),
		})
	}
	return msgs
}

// countKeys counts unique keys from given Message slice.
func countKeys(msgs []Message) int {
	m := make(map[string]struct{})
	for _, msg := range msgs {
		m[string(msg.Key)] = struct{}{}
	}
	return len(m)
}

func createTopicWithCompaction(t *testing.T, topic string, partitions int) {
	t.Helper()

	t.Logf("createTopic(%s, %d)", topic, partitions)

	conn, err := Dial("tcp", "localhost:9092")
	require.NoError(t, err)
	defer conn.Close()

	controller, err := conn.Controller()
	require.NoError(t, err)

	conn, err = Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	require.NoError(t, err)

	conn.SetDeadline(time.Now().Add(10 * time.Second))

	err = conn.CreateTopics(TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
		ConfigEntries: []ConfigEntry{
			{
				ConfigName:  "cleanup.policy",
				ConfigValue: "compact",
			},
			{
				ConfigName:  "segment.bytes",
				ConfigValue: "200",
			},
		},
	})
	if err != nil {
		if !errors.Is(err, TopicAlreadyExists) {
			require.NoError(t, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	waitForTopic(ctx, t, topic)
}

// The current behavior of the Reader is to retry OffsetOutOfRange errors
// indefinitely, which results in programs hanging in the event of a topic being
// re-created while a consumer is running. To retain backwards-compatibility,
// ReaderConfig.OffsetOutOfRangeError is being used to instruct the Reader to
// return an error in this case instead, allowing callers to react.
func testReaderTopicRecreated(t *testing.T, ctx context.Context, r *Reader) {
	r.config.OffsetOutOfRangeError = true

	topic := r.config.Topic

	// add 1 message to the topic
	prepareReader(t, ctx, r, makeTestSequence(1)...)

	// consume the message (moving the offset from 0 -> 1)
	_, err := r.ReadMessage(ctx)
	require.NoError(t, err)

	// destroy the topic, then recreate it so the offset now becomes 0
	deleteTopic(t, topic)
	createTopic(t, topic, 1)

	// expect an error, since the offset should now be out of range
	_, err = r.ReadMessage(ctx)
	require.ErrorIs(t, err, OffsetOutOfRange)
}
