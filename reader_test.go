package kafka

import (
	"context"
	"io"
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

	// there's a possible go routine scheduling order whereby the stats have not
	// been fully updated yet and the following assertions would fail if we
	// retrieved stats immediately.  the issue rarely happens locally but
	// happens with some degree of regularity in CI.  we don't have a way
	// to ensure stats are updated, so approximating it with a sleep. :|
	time.Sleep(10 * time.Millisecond)

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
		Errors:        1, // because the configured timeout is < defaultRTT, so fetch timeouts get logged as errors
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

		// TODO: remove when we get rid of the deprecated field.
		DeprecatedFetchesWithTypo: 1,
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

	_, err = conn.createTopics(createTopicsRequestV0{
		Topics: []createTopicsRequestV0Topic{
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

	t.Parallel()
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
	prepareReader(t, ctx, r)
	groupID := r.Config().GroupID

	// wait for generationID > 0 so we know our reader has joined the group
	membershipTimer := time.After(5 * time.Second)
	for {
		done := false
		select {
		case <-membershipTimer:
			t.Fatalf("our reader never joind its group")
		default:
			generationID, _ := r.membership()
			if generationID > 0 {
				done = true
			}
		}
		if done {
			break
		}
	}

	err := r.Close()
	if err != nil {
		t.Fatalf("unexpected error closing reader: %s", err.Error())
	}

	conn, err := Dial("tcp", "localhost:9092")
	if err != nil {
		t.Fatalf("error dialing: %v", err)
	}
	resp, err := conn.describeGroups(describeGroupsRequestV0{
		GroupIDs: []string{groupID},
	})
	if err != nil {
		t.Fatalf("error from describeGroups %v", err)
	}
	if len(resp.Groups) != 1 {
		t.Fatalf("expected 1 group. got: %d", len(resp.Groups))
	}
	if len(resp.Groups[0].Members) != 0 {
		t.Fatalf("expected group membership size of %d, but got %d", 0, len(resp.Groups[0].Members))
	}
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
	if err := r.SetOffset(LastOffset); err != errNotAvailableWithGroup {
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

	newJoinGroupResponseV1 := func(topicsByMemberID map[string][]string) joinGroupResponseV1 {
		resp := joinGroupResponseV1{
			GroupProtocol: RoundRobinGroupBalancer{}.ProtocolName(),
		}

		for memberID, topics := range topicsByMemberID {
			resp.Members = append(resp.Members, joinGroupResponseMemberV1{
				MemberID: memberID,
				MemberMetadata: groupMetadata{
					Topics: topics,
				}.bytes(),
			})
		}

		return resp
	}

	testCases := map[string]struct {
		Members     joinGroupResponseV1
		Assignments GroupMemberAssignments
	}{
		"nil": {
			Members:     newJoinGroupResponseV1(nil),
			Assignments: GroupMemberAssignments{},
		},
		"one member, one topic": {
			Members: newJoinGroupResponseV1(map[string][]string{
				"member-1": {"topic-1"},
			}),
			Assignments: GroupMemberAssignments{
				"member-1": map[string][]int{
					"topic-1": {0, 1, 2},
				},
			},
		},
		"one member, two topics": {
			Members: newJoinGroupResponseV1(map[string][]string{
				"member-1": {"topic-1", "topic-2"},
			}),
			Assignments: GroupMemberAssignments{
				"member-1": map[string][]int{
					"topic-1": {0, 1, 2},
					"topic-2": {0},
				},
			},
		},
		"two members, one topic": {
			Members: newJoinGroupResponseV1(map[string][]string{
				"member-1": {"topic-1"},
				"member-2": {"topic-1"},
			}),
			Assignments: GroupMemberAssignments{
				"member-1": map[string][]int{
					"topic-1": {0, 2},
				},
				"member-2": map[string][]int{
					"topic-1": {1},
				},
			},
		},
		"two members, two unshared topics": {
			Members: newJoinGroupResponseV1(map[string][]string{
				"member-1": {"topic-1"},
				"member-2": {"topic-2"},
			}),
			Assignments: GroupMemberAssignments{
				"member-1": map[string][]int{
					"topic-1": {0, 1, 2},
				},
				"member-2": map[string][]int{
					"topic-2": {0},
				},
			},
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			r := &Reader{}
			r.config.GroupBalancers = []GroupBalancer{
				RangeGroupBalancer{},
				RoundRobinGroupBalancer{},
			}
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
			scenario:   "consumer group notices when partitions are added",
			partitions: 2,
			function:   testReaderConsumerGroupRebalanceOnPartitionAdd,
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

	conn, err := r.coordinator()
	if err != nil {
		t.Errorf("unable to connect to coordinator: %v", err)
	}
	defer conn.Close()

	offsets, err := r.fetchOffsets(conn, map[string][]int32{
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
	if err := r.CommitMessages(ctx, m); err != nil {
		t.Errorf("bad commit message: %v", err)
	}
	if elapsed := time.Now().Sub(started); elapsed > 10*time.Millisecond {
		t.Errorf("background commits should happen nearly instantly")
	}

	// wait for committer to pick up the commits
	time.Sleep(r.config.CommitInterval * 3)

	conn, err := r.coordinator()
	if err != nil {
		t.Errorf("unable to connect to coordinator: %v", err)
	}
	defer conn.Close()

	offsets, err := r.fetchOffsets(conn, map[string][]int32{
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

	if err := r.CommitMessages(ctx, m); err != nil {
		t.Errorf("bad commit message: %v", err)
	}

	if err := r.Close(); err != nil {
		t.Errorf("bad Close: %v", err)
	}

	r2 := NewReader(r.config)
	defer r2.Close()

	conn, err := r2.coordinator()
	if err != nil {
		t.Errorf("unable to connect to coordinator: %v", err)
	}
	defer conn.Close()

	offsets, err := r2.fetchOffsets(conn, map[string][]int32{
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

// Build a struct to implement the ReadPartitions interface.
type MockConnWatcher struct {
	count      int
	partitions [][]Partition
}

func (m *MockConnWatcher) ReadPartitions(topics ...string) (partitions []Partition, err error) {
	partitions = m.partitions[m.count]
	// cap the count at len(partitions) -1 so ReadPartitions doesn't even go out of bounds
	// and long running tests don't fail
	if m.count < len(m.partitions) {
		m.count++
	}

	return partitions, err
}

func testReaderConsumerGroupRebalanceOnPartitionAdd(t *testing.T, ctx context.Context, r *Reader) {
	// Sadly this test is time based, so at the end will be seeing if the runGroup run to completion within the
	// allotted time. The allotted time is 4x the PartitionWatchInterval.
	now := time.Now()
	watchTime := 500 * time.Millisecond
	conn := &MockConnWatcher{
		partitions: [][]Partition{
			{
				Partition{
					Topic: "topic-1",
					ID:    0,
				},
			},
			{
				Partition{
					Topic: "topic-1",
					ID:    0,
				},
				{
					Topic: "topic-1",
					ID:    1,
				},
			},
		},
	}

	rg := &runGroup{}
	rg = rg.WithContext(ctx)
	r.config.PartitionWatchInterval = watchTime
	rg.Go(r.partitionWatcher(conn))
	rg.Wait()
	if time.Now().Sub(now).Seconds() > r.config.PartitionWatchInterval.Seconds()*4 {
		t.Error("partitionWatcher didn't see update")
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

	// write messages across both partitions
	writer := NewWriter(WriterConfig{
		Brokers:   r.config.Brokers,
		Topic:     r.config.Topic,
		Dialer:    r.config.Dialer,
		Balancer:  &RoundRobin{},
		BatchSize: 1,
	})
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

type mockOffsetCommitter struct {
	invocations int
	failCount   int
	err         error
}

func (m *mockOffsetCommitter) offsetCommit(request offsetCommitRequestV2) (offsetCommitResponseV2, error) {
	m.invocations++

	if m.failCount > 0 {
		m.failCount--
		return offsetCommitResponseV2{}, io.EOF
	}

	return offsetCommitResponseV2{}, nil
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
		{config: ReaderConfig{Brokers: []string{"broker1"}, Topic: "topic1", Partition: 0, MinBytes: 5, MaxBytes: 6, GroupID: "group1", HeartbeatInterval: 2, SessionTimeout: -1}, errorOccured: true},
		{config: ReaderConfig{Brokers: []string{"broker1"}, Topic: "topic1", Partition: 0, MinBytes: 5, MaxBytes: 6, GroupID: "group1", HeartbeatInterval: 2, SessionTimeout: 2, RebalanceTimeout: -2}, errorOccured: true},
		{config: ReaderConfig{Brokers: []string{"broker1"}, Topic: "topic1", Partition: 0, MinBytes: 5, MaxBytes: 6, GroupID: "group1", HeartbeatInterval: 2, SessionTimeout: 2, RebalanceTimeout: 2, RetentionTime: -1}, errorOccured: true},
		{config: ReaderConfig{Brokers: []string{"broker1"}, Topic: "topic1", Partition: 0, MinBytes: 5, MaxBytes: 6, GroupID: "group1", HeartbeatInterval: 2, SessionTimeout: 2, RebalanceTimeout: 2, RetentionTime: 1, CommitInterval: -1}, errorOccured: true},
		{config: ReaderConfig{Brokers: []string{"broker1"}, Topic: "topic1", Partition: 0, MinBytes: 5, MaxBytes: 6, GroupID: "group1", HeartbeatInterval: 2, SessionTimeout: 2, RebalanceTimeout: 2, RetentionTime: 1, CommitInterval: 1, PartitionWatchInterval: -1}, errorOccured: true},
		{config: ReaderConfig{Brokers: []string{"broker1"}, Topic: "topic1", Partition: 0, MinBytes: 5, MaxBytes: 6, GroupID: "group1", HeartbeatInterval: 2, SessionTimeout: 2, RebalanceTimeout: 2, RetentionTime: 1, CommitInterval: 1, PartitionWatchInterval: 1}, errorOccured: false},
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
			conn := &mockOffsetCommitter{failCount: test.Fails}

			r := &Reader{stctx: context.Background()}
			err := r.commitOffsetsWithRetry(conn, offsets, defaultCommitRetries)
			switch {
			case test.HasError && err == nil:
				t.Error("bad err: expected not nil; got nil")
			case !test.HasError && err != nil:
				t.Errorf("bad err: expected nil; got %v", err)
			}
			if test.Invocations != conn.invocations {
				t.Errorf("expected %v retries; got %v", test.Invocations, conn.invocations)
			}
		})
	}
}

// Test that a reader won't continually rebalance when there are more consumers
// than partitions in a group.
// https://github.com/segmentio/kafka-go/issues/200
func TestRebalanceTooManyConsumers(t *testing.T) {
	t.Parallel()

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
	t.Parallel()
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
	createTopic(t, conf.Topic, 1)

	w := NewWriter(WriterConfig{
		Brokers:      r.config.Brokers,
		Topic:        r.config.Topic,
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    1,
	})
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
