package kafka

import (
	"context"
	"math/rand"
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
