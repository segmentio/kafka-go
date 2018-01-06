package kafka

import (
	"context"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestReader(t *testing.T) {
	t.Parallel()

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
			scenario: "setting the offset to random values returns the expected messages when Read is called",
			function: testReaderSetRandomOffset,
		},

		{
			scenario: "calling Lag returns the lag of the last message read from kafka",
			function: testReaderLag,
		},

		{
			scenario: "calling ReadLag returns the current lag of a reader",
			function: testReaderReadLag,
		},

		{
			scenario: "calling Stats returns accurate stats about the reader",
			function: testReaderStats,
		},

		{ // https://github.com/segmentio/kafka-go/issues/30
			scenario: "reading from an out-of-range offset waits until the context is cancelled",
			function: testReaderOutOfRangeGetsCanceled,
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
			})
			defer r.Close()
			testFunc(t, ctx, r)
		})
	}
}

func testReaderReadCanceled(t *testing.T, ctx context.Context, r *Reader) {
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	if _, err := r.ReadMessage(ctx); err != context.Canceled {
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

func testReaderStats(t *testing.T, ctx context.Context, r *Reader) {
	const N = 10
	prepareReader(t, ctx, r, makeTestSequence(N)...)

	var offset int64
	var bytes int64

	for i := 0; i != N; i++ {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Error("reading message at offset", offset, "failed:", err)
			return
		}
		offset = m.Offset + 1
		bytes += int64(len(m.Key) + len(m.Value))
	}

	stats := r.Stats()

	// First verify that metrics with unpredictable values are not zero.
	if stats.DialTime == (DurationStats{}) {
		t.Error("no dial time reported by reader stats")
	}
	if stats.ReadTime == (DurationStats{}) {
		t.Error("no read time reported by reader stats")
	}
	if stats.WaitTime == (DurationStats{}) {
		t.Error("no wait time reported by reader stats")
	}
	if len(stats.Topic) == 0 {
		t.Error("empty topic in reader stats")
	}

	// Then compare all remaining metrics.
	expect := ReaderStats{
		Dials:         1,
		Fetches:       1,
		Messages:      10,
		Bytes:         10,
		Rebalances:    0,
		Timeouts:      0,
		Errors:        0,
		DialTime:      stats.DialTime,
		ReadTime:      stats.ReadTime,
		WaitTime:      stats.WaitTime,
		FetchSize:     SummaryStats{Avg: 10, Min: 10, Max: 10},
		FetchBytes:    SummaryStats{Avg: 10, Min: 10, Max: 10},
		Offset:        10,
		Lag:           0,
		MinBytes:      1,
		MaxBytes:      10000000,
		MaxWait:       100 * time.Millisecond,
		QueueLength:   0,
		QueueCapacity: 100,
		ClientID:      "",
		Topic:         stats.Topic,
		Partition:     "0",
	}

	if stats != expect {
		t.Error("bad stats:")
		t.Log("expected:", expect)
		t.Log("found:   ", stats)
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
	if err != context.DeadlineExceeded {
		t.Error("bad error:", err)
	}

	t1 := time.Now()

	if d := t1.Sub(t0); d < D {
		t.Error("ReadMessage returned too early after", d)
	}
}

func createTopic(t *testing.T, topic string, partitions int) {
	conn, err := Dial("tcp", "localhost:9092")
	if err != nil {
		t.Error("bad conn")
		return
	}
	defer conn.Close()

	_, err = conn.createTopics(createTopicsRequestV2{
		Topics: []createTopicsRequestV2Topic{
			{
				Topic:             topic,
				NumPartitions:     int32(partitions),
				ReplicationFactor: 1,
			},
		},
		Timeout: int32(30 * time.Second / time.Millisecond),
	})
	switch err {
	case nil:
		// ok
	case TopicAlreadyExists:
		// ok
	default:
		t.Error("bad createTopics", err)
		t.FailNow()
	}
}

func TestReaderOnNonZeroPartition(t *testing.T) {
	t.Parallel()

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

func makeTestSequence(n int) []Message {
	msgs := make([]Message, n)
	for i := 0; i != n; i++ {
		msgs[i] = Message{
			Value: []byte(strconv.Itoa(i)),
		}
	}
	return msgs
}

func prepareReader(t *testing.T, ctx context.Context, r *Reader, msgs ...Message) {
	var config = r.Config()
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
	benchmarkReaderPayload = make([]byte, 16*1024)
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

	for i := 0; i != b.N; i++ {
		if (i % 10000) == 0 {
			r.SetOffset(0)
		}
		r.ReadMessage(ctx)
	}

	r.Close()
	b.SetBytes(int64(len(benchmarkReaderPayload)))
}

func TestConsumerGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T, context.Context, *Reader)
	}{
		{
			scenario: "Close immediately after NewReader",
			function: testConsumerGroupImmediateClose,
		},

		{
			scenario: "Close immediately after NewReader",
			function: testConsumerGroupSimple,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			topic := makeTopic()
			createTopic(t, topic, 1)

			r := NewReader(ReaderConfig{
				Brokers:  []string{"localhost:9092"},
				Topic:    topic,
				GroupID:  makeGroupID(),
				MinBytes: 1,
				MaxBytes: 10e6,
				MaxWait:  100 * time.Millisecond,
			})
			defer r.Close()
			testFunc(t, ctx, r)
		})
	}

	const broker = "localhost:9092"

	topic := makeTopic()
	createTopic(t, topic, 1)

	r := NewReader(ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,
		GroupID: makeGroupID(),
	})
	r.Close()
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
	if err := r.SetOffset(-1); err != errNotAvailable {
		t.Fatalf("expected %v; got %v", errNotAvailable, err)
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
	newMeta := func(memberID string, topics ...string) memberGroupMetadata {
		return memberGroupMetadata{
			MemberID: memberID,
			Metadata: groupMetadata{
				Topics: topics,
			},
		}
	}

	testCases := map[string]struct {
		Members []memberGroupMetadata
		Topics  []string
	}{
		"nil": {},
		"single member, single topic": {
			Members: []memberGroupMetadata{
				newMeta("a", "topic"),
			},
			Topics: []string{"topic"},
		},
		"two members, single topic": {
			Members: []memberGroupMetadata{
				newMeta("a", "topic"),
				newMeta("b", "topic"),
			},
			Topics: []string{"topic"},
		},
		"two members, two topics": {
			Members: []memberGroupMetadata{
				newMeta("a", "topic-1"),
				newMeta("b", "topic-2"),
			},
			Topics: []string{"topic-1", "topic-2"},
		},
		"three members, three shared topics": {
			Members: []memberGroupMetadata{
				newMeta("a", "topic-1", "topic-2"),
				newMeta("b", "topic-2", "topic-3"),
				newMeta("c", "topic-3", "topic-1"),
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

func TestReaderAssignTopicPartitions(t *testing.T) {
	conn := &MockConn{
		partitions: []Partition{
			{
				Topic: "topic-1",
				ID:    0,
			},
			{
				Topic: "topic-1",
				ID:    1,
			},
			{
				Topic: "topic-1",
				ID:    2,
			},
			{
				Topic: "topic-2",
				ID:    0,
			},
		},
	}

	newJoinGroupResponseV2 := func(topicsByMemberID map[string][]string) joinGroupResponseV2 {
		resp := joinGroupResponseV2{
			GroupProtocol: roundrobinStrategy{}.ProtocolName(),
		}

		for memberID, topics := range topicsByMemberID {
			resp.Members = append(resp.Members, joinGroupResponseMemberV2{
				MemberID: memberID,
				MemberMetadata: groupMetadata{
					Topics: topics,
				}.bytes(),
			})
		}

		return resp
	}

	testCases := map[string]struct {
		Members     joinGroupResponseV2
		Assignments memberGroupAssignments
	}{
		"nil": {
			Members:     newJoinGroupResponseV2(nil),
			Assignments: memberGroupAssignments{},
		},
		"one member, one topic": {
			Members: newJoinGroupResponseV2(map[string][]string{
				"member-1": {"topic-1"},
			}),
			Assignments: memberGroupAssignments{
				"member-1": map[string][]int32{
					"topic-1": {0, 1, 2},
				},
			},
		},
		"one member, two topics": {
			Members: newJoinGroupResponseV2(map[string][]string{
				"member-1": {"topic-1", "topic-2"},
			}),
			Assignments: memberGroupAssignments{
				"member-1": map[string][]int32{
					"topic-1": {0, 1, 2},
					"topic-2": {0},
				},
			},
		},
		"two members, one topic": {
			Members: newJoinGroupResponseV2(map[string][]string{
				"member-1": {"topic-1"},
				"member-2": {"topic-1"},
			}),
			Assignments: memberGroupAssignments{
				"member-1": map[string][]int32{
					"topic-1": {0, 2},
				},
				"member-2": map[string][]int32{
					"topic-1": {1},
				},
			},
		},
		"two members, two unshared topics": {
			Members: newJoinGroupResponseV2(map[string][]string{
				"member-1": {"topic-1"},
				"member-2": {"topic-2"},
			}),
			Assignments: memberGroupAssignments{
				"member-1": map[string][]int32{
					"topic-1": {0, 1, 2},
				},
				"member-2": map[string][]int32{
					"topic-2": {0},
				},
			},
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			r := &Reader{}
			assignments, err := r.assignTopicPartitions(conn, tc.Members)
			if err != nil {
				t.Fatalf("bad err: %v", err)
			}
			if !reflect.DeepEqual(tc.Assignments, assignments) {
				t.Errorf("expected %v; got %v", tc.Assignments, assignments)
			}
		})
	}
}

func TestReaderConsumerGroup(t *testing.T) {
	t.Parallel()

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
			scenario:       "verify outstanding offsets committed on close",
			partitions:     1,
			commitInterval: time.Minute,
			function:       testReaderConsumerGroupVerifyCommitsOnClose,
		},

		{
			scenario:   "read content across partitions",
			partitions: 3,
			function:   testReaderConsumerGroupReadContentAcrossPartitions,
		},

		{
			scenario:   "rebalance two readers",
			partitions: 2,
			function:   testReaderConsumerGroupRebalance,
		},

		{
			scenario:   "rebalance two readers across topics; reader 1 => topic 1, reader 2 => topic 2",
			partitions: 2,
			function:   testReaderConsumerGroupRebalanceAcrossTopics,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			topic := makeTopic()
			createTopic(t, topic, test.partitions)

			groupID := makeGroupID()
			r := NewReader(ReaderConfig{
				Brokers:           []string{"localhost:9092"},
				Topic:             topic,
				GroupID:           groupID,
				HeartbeatInterval: time.Second,
				CommitInterval:    test.commitInterval,
				SessionTimeout:    time.Second * 6,
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

	if err := r.CommitMessage(m); err != nil {
		t.Errorf("bad commit message: %v", err)
	}

	offsets, err := r.fetchOffsets(map[string][]int32{
		r.config.Topic: {0},
	})
	if err != nil {
		t.Errorf("bad fetchOffsets: %v", err)
	}

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
	if err := r.CommitMessage(m); err != nil {
		t.Errorf("bad commit message: %v", err)
	}
	if elapsed := time.Now().Sub(started); elapsed > 10*time.Millisecond {
		t.Errorf("background commits should happen nearly instantly")
	}

	// wait for committer to pick up the commits
	time.Sleep(r.config.CommitInterval * 3)

	offsets, err := r.fetchOffsets(map[string][]int32{
		r.config.Topic: {0},
	})
	if err != nil {
		t.Errorf("bad fetchOffsets: %v", err)
	}

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

	if err := r.CommitMessage(m); err != nil {
		t.Errorf("bad commit message: %v", err)
	}

	if err := r.Close(); err != nil {
		t.Errorf("bad Close: %v", err)
	}

	r2 := NewReader(r.config)
	defer r2.Close()

	offsets, err := r2.fetchOffsets(map[string][]int32{
		r.config.Topic: {0},
	})
	if err != nil {
		t.Errorf("bad fetchOffsets: %v", err)
	}

	if expected := map[int]int64{0: m.Offset + 1}; !reflect.DeepEqual(expected, offsets) {
		t.Errorf("expected %v; got %v", expected, offsets)
	}
}

func testReaderConsumerGroupReadContentAcrossPartitions(t *testing.T, ctx context.Context, r *Reader) {
	const N = 12

	writer := NewWriter(WriterConfig{
		Brokers:   r.config.Brokers,
		Topic:     r.config.Topic,
		Dialer:    r.config.Dialer,
		Balancer:  &RoundRobin{},
		BatchSize: 1,
	})
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

	// rebalance should result in 12 message in each of the partitions
	writer := NewWriter(WriterConfig{
		Brokers:   r.config.Brokers,
		Topic:     r.config.Topic,
		Dialer:    r.config.Dialer,
		Balancer:  &RoundRobin{},
		BatchSize: 1,
	})
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
	topic2 := makeTopic()
	createTopic(t, topic2, 1)

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
	writer := NewWriter(WriterConfig{
		Brokers:   r.config.Brokers,
		Topic:     r.config.Topic,
		Dialer:    r.config.Dialer,
		Balancer:  &RoundRobin{},
		BatchSize: 1,
	})
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
