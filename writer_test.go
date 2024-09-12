package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/sasl/plain"
)

func TestBatchQueue(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			scenario: "the remaining items in a queue can be gotten after closing",
			function: testBatchQueueGetWorksAfterClose,
		},
		{
			scenario: "putting into a closed queue fails",
			function: testBatchQueuePutAfterCloseFails,
		},
		{
			scenario: "putting into a queue awakes a goroutine in a get call",
			function: testBatchQueuePutWakesSleepingGetter,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()
			testFunc(t)
		})
	}
}

func testBatchQueuePutWakesSleepingGetter(t *testing.T) {
	bq := newBatchQueue(10)
	var wg sync.WaitGroup
	ready := make(chan struct{})
	var batch *writeBatch
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(ready)
		batch = bq.Get()
	}()
	<-ready
	bq.Put(newWriteBatch(time.Now(), time.Hour*100))
	wg.Wait()
	if batch == nil {
		t.Fatal("got nil batch")
	}
}

func testBatchQueuePutAfterCloseFails(t *testing.T) {
	bq := newBatchQueue(10)
	bq.Close()
	if put := bq.Put(newWriteBatch(time.Now(), time.Hour*100)); put {
		t.Fatal("put batch into closed queue")
	}
}

func testBatchQueueGetWorksAfterClose(t *testing.T) {
	bq := newBatchQueue(10)
	enqueueBatches := []*writeBatch{
		newWriteBatch(time.Now(), time.Hour*100),
		newWriteBatch(time.Now(), time.Hour*100),
	}

	for _, batch := range enqueueBatches {
		put := bq.Put(batch)
		if !put {
			t.Fatal("failed to put batch into queue")
		}
	}

	bq.Close()

	batchesGotten := 0
	for batchesGotten != 2 {
		dequeueBatch := bq.Get()
		if dequeueBatch == nil {
			t.Fatalf("no batch returned from get")
		}
		batchesGotten++
	}
}

func TestWriter(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			scenario: "closing a writer right after creating it returns promptly with no error",
			function: testWriterClose,
		},

		{
			scenario: "writing 1 message through a writer using round-robin balancing produces 1 message to the first partition",
			function: testWriterRoundRobin1,
		},

		{
			scenario: "running out of max attempts should return an error",
			function: testWriterMaxAttemptsErr,
		},

		{
			scenario: "writing a message larger then the max bytes should return an error",
			function: testWriterMaxBytes,
		},

		{
			scenario: "writing a batch of message based on batch byte size",
			function: testWriterBatchBytes,
		},

		{
			scenario: "writing a batch of messages",
			function: testWriterBatchSize,
		},

		{
			scenario: "writing messages with a small batch byte size",
			function: testWriterSmallBatchBytes,
		},
		{
			scenario: "writing messages with headers",
			function: testWriterBatchBytesHeaders,
		},
		{
			scenario: "setting a non default balancer on the writer",
			function: testWriterSetsRightBalancer,
		},
		{
			scenario: "setting RequiredAcks to None in Writer does not cause a panic",
			function: testWriterRequiredAcksNone,
		},
		{
			scenario: "writing messages to multiple topics",
			function: testWriterMultipleTopics,
		},
		{
			scenario: "writing messages without specifying a topic",
			function: testWriterMissingTopic,
		},
		{
			scenario: "specifying topic for message when already set for writer",
			function: testWriterUnexpectedMessageTopic,
		},
		{
			scenario: "writing a message to an invalid partition",
			function: testWriterInvalidPartition,
		},
		{
			scenario: "writing a message to a non-existent topic creates the topic",
			function: testWriterAutoCreateTopic,
		},
		{
			scenario: "terminates on an attempt to write a message to a nonexistent topic",
			function: testWriterTerminateMissingTopic,
		},
		{
			scenario: "writing a message with SASL Plain authentication",
			function: testWriterSasl,
		},
		{
			scenario: "test default configuration values",
			function: testWriterDefaults,
		},
		{
			scenario: "test default stats values",
			function: testWriterDefaultStats,
		},
		{
			scenario: "test stats values with override config",
			function: testWriterOverrideConfigStats,
		},
		{
			scenario: "test write message with writer data",
			function: testWriteMessageWithWriterData,
		},
		{
			scenario: "test no new partition writers after close",
			function: testWriterNoNewPartitionWritersAfterClose,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()
			testFunc(t)
		})
	}
}

func newTestWriter(config WriterConfig) *Writer {
	if len(config.Brokers) == 0 {
		config.Brokers = []string{"localhost:9092"}
	}
	return NewWriter(config)
}

func testWriterClose(t *testing.T) {
	const topic = "test-writer-0"
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	w := newTestWriter(WriterConfig{
		Topic: topic,
	})

	if err := w.Close(); err != nil {
		t.Error(err)
	}
}

func testWriterRequiredAcksNone(t *testing.T) {
	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	transport := &Transport{}
	defer transport.CloseIdleConnections()

	writer := &Writer{
		Addr:         TCP("localhost:9092"),
		Topic:        topic,
		Balancer:     &RoundRobin{},
		RequiredAcks: RequireNone,
		Transport:    transport,
	}
	defer writer.Close()

	msg := Message{
		Key:   []byte("ThisIsAKey"),
		Value: []byte("Test message for required acks test"),
	}

	err := writer.WriteMessages(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
}

func testWriterSetsRightBalancer(t *testing.T) {
	const topic = "test-writer-1"
	balancer := &CRC32Balancer{}
	w := newTestWriter(WriterConfig{
		Topic:    topic,
		Balancer: balancer,
	})
	defer w.Close()

	if w.Balancer != balancer {
		t.Errorf("Balancer not set correctly")
	}
}

func testWriterRoundRobin1(t *testing.T) {
	const topic = "test-writer-1"
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	offset, err := readOffset(topic, 0)
	if err != nil {
		t.Fatal(err)
	}

	w := newTestWriter(WriterConfig{
		Topic:    topic,
		Balancer: &RoundRobin{},
	})
	defer w.Close()

	if err := w.WriteMessages(context.Background(), Message{
		Value: []byte("Hello World!"),
	}); err != nil {
		t.Error(err)
		return
	}

	msgs, err := readPartition(topic, 0, offset)
	if err != nil {
		t.Error("error reading partition", err)
		return
	}

	if len(msgs) != 1 {
		t.Error("bad messages in partition", msgs)
		return
	}

	for _, m := range msgs {
		if string(m.Value) != "Hello World!" {
			t.Error("bad messages in partition", msgs)
			break
		}
	}
}

func TestValidateWriter(t *testing.T) {
	tests := []struct {
		config       WriterConfig
		errorOccured bool
	}{
		{config: WriterConfig{}, errorOccured: true},
		{config: WriterConfig{Brokers: []string{"broker1", "broker2"}}, errorOccured: false},
		{config: WriterConfig{Brokers: []string{"broker1"}, Topic: "topic1"}, errorOccured: false},
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

func testWriterMaxAttemptsErr(t *testing.T) {
	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	w := newTestWriter(WriterConfig{
		Brokers:     []string{"localhost:9999"}, // nothing is listening here
		Topic:       topic,
		MaxAttempts: 3,
		Balancer:    &RoundRobin{},
	})
	defer w.Close()

	if err := w.WriteMessages(context.Background(), Message{
		Value: []byte("Hello World!"),
	}); err == nil {
		t.Error("expected error")
		return
	}
}

func testWriterMaxBytes(t *testing.T) {
	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	w := newTestWriter(WriterConfig{
		Topic:      topic,
		BatchBytes: 25,
	})
	defer w.Close()

	if err := w.WriteMessages(context.Background(), Message{
		Value: []byte("Hi"),
	}); err != nil {
		t.Error(err)
		return
	}

	firstMsg := []byte("Hello World!")
	secondMsg := []byte("LeftOver!")
	msgs := []Message{
		{
			Value: firstMsg,
		},
		{
			Value: secondMsg,
		},
	}
	if err := w.WriteMessages(context.Background(), msgs...); err == nil {
		t.Error("expected error")
		return
	} else if err != nil {
		var e MessageTooLargeError
		switch {
		case errors.As(err, &e):
			if string(e.Message.Value) != string(firstMsg) {
				t.Errorf("unxpected returned message. Expected: %s, Got %s", firstMsg, e.Message.Value)
				return
			}
			if len(e.Remaining) != 1 {
				t.Error("expected remaining errors; found none")
				return
			}
			if string(e.Remaining[0].Value) != string(secondMsg) {
				t.Errorf("unxpected returned message. Expected: %s, Got %s", secondMsg, e.Message.Value)
				return
			}

		default:
			t.Errorf("unexpected error: %s", err)
			return
		}
	}
}

// readOffset gets the latest offset for the given topic/partition.
func readOffset(topic string, partition int) (offset int64, err error) {
	var conn *Conn

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if conn, err = DialLeader(ctx, "tcp", "localhost:9092", topic, partition); err != nil {
		err = fmt.Errorf("readOffset, DialLeader: %w", err)
		return
	}
	defer conn.Close()

	offset, err = conn.ReadLastOffset()
	if err != nil {
		err = fmt.Errorf("readOffset, conn.ReadLastOffset: %w", err)
	}
	return
}

func readPartition(topic string, partition int, offset int64) (msgs []Message, err error) {
	var conn *Conn

	if conn, err = DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition); err != nil {
		return
	}
	defer conn.Close()

	conn.Seek(offset, SeekAbsolute)
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := conn.ReadBatch(0, 1000000000)
	defer batch.Close()

	for {
		var msg Message

		if msg, err = batch.ReadMessage(); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return
		}

		msgs = append(msgs, msg)
	}
}

func testWriterBatchBytes(t *testing.T) {
	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	offset, err := readOffset(topic, 0)
	if err != nil {
		t.Fatal(err)
	}

	w := newTestWriter(WriterConfig{
		Topic:        topic,
		BatchBytes:   50,
		BatchTimeout: math.MaxInt32 * time.Second,
		Balancer:     &RoundRobin{},
	})
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, []Message{
		{Value: []byte("M0")}, // 25 Bytes
		{Value: []byte("M1")}, // 25 Bytes
		{Value: []byte("M2")}, // 25 Bytes
		{Value: []byte("M3")}, // 25 Bytes
	}...); err != nil {
		t.Error(err)
		return
	}

	if w.Stats().Writes != 2 {
		t.Error("didn't create expected batches")
		return
	}
	msgs, err := readPartition(topic, 0, offset)
	if err != nil {
		t.Error("error reading partition", err)
		return
	}

	if len(msgs) != 4 {
		t.Error("bad messages in partition", msgs)
		return
	}

	for i, m := range msgs {
		if string(m.Value) == "M"+strconv.Itoa(i) {
			continue
		}
		t.Error("bad messages in partition", string(m.Value))
	}
}

func testWriterBatchSize(t *testing.T) {
	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	offset, err := readOffset(topic, 0)
	if err != nil {
		t.Fatal(err)
	}

	w := newTestWriter(WriterConfig{
		Topic:        topic,
		BatchSize:    2,
		BatchTimeout: math.MaxInt32 * time.Second,
		Balancer:     &RoundRobin{},
	})
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, []Message{
		{Value: []byte("Hi")}, // 24 Bytes
		{Value: []byte("By")}, // 24 Bytes
	}...); err != nil {
		t.Error(err)
		return
	}

	if w.Stats().Writes > 1 {
		t.Error("didn't batch messages")
		return
	}
	msgs, err := readPartition(topic, 0, offset)
	if err != nil {
		t.Error("error reading partition", err)
		return
	}

	if len(msgs) != 2 {
		t.Error("bad messages in partition", msgs)
		return
	}

	for _, m := range msgs {
		if string(m.Value) == "Hi" || string(m.Value) == "By" {
			continue
		}
		t.Error("bad messages in partition", msgs)
	}
}

func testWriterSmallBatchBytes(t *testing.T) {
	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	offset, err := readOffset(topic, 0)
	if err != nil {
		t.Fatal(err)
	}

	w := newTestWriter(WriterConfig{
		Topic:        topic,
		BatchBytes:   25,
		BatchTimeout: 50 * time.Millisecond,
		Balancer:     &RoundRobin{},
	})
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, []Message{
		{Value: []byte("Hi")}, // 24 Bytes
		{Value: []byte("By")}, // 24 Bytes
	}...); err != nil {
		t.Error(err)
		return
	}
	ws := w.Stats()
	if ws.Writes != 2 {
		t.Error("didn't batch messages; Writes: ", ws.Writes)
		return
	}
	msgs, err := readPartition(topic, 0, offset)
	if err != nil {
		t.Error("error reading partition", err)
		return
	}

	if len(msgs) != 2 {
		t.Error("bad messages in partition", msgs)
		return
	}

	for _, m := range msgs {
		if string(m.Value) == "Hi" || string(m.Value) == "By" {
			continue
		}
		t.Error("bad messages in partition", msgs)
	}
}

func testWriterBatchBytesHeaders(t *testing.T) {
	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	offset, err := readOffset(topic, 0)
	if err != nil {
		t.Fatal(err)
	}

	w := newTestWriter(WriterConfig{
		Topic:        topic,
		BatchBytes:   100,
		BatchTimeout: 50 * time.Millisecond,
		Balancer:     &RoundRobin{},
	})
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, []Message{
		{
			Value: []byte("Hello World 1"),
			Headers: []Header{
				{Key: "User-Agent", Value: []byte("abc/xyz")},
			},
		},
		{
			Value: []byte("Hello World 2"),
			Headers: []Header{
				{Key: "User-Agent", Value: []byte("abc/xyz")},
			},
		},
	}...); err != nil {
		t.Error(err)
		return
	}
	ws := w.Stats()
	if ws.Writes != 2 {
		t.Error("didn't batch messages; Writes: ", ws.Writes)
		return
	}
	msgs, err := readPartition(topic, 0, offset)
	if err != nil {
		t.Error("error reading partition", err)
		return
	}

	if len(msgs) != 2 {
		t.Error("bad messages in partition", msgs)
		return
	}

	for _, m := range msgs {
		if strings.HasPrefix(string(m.Value), "Hello World") {
			continue
		}
		t.Error("bad messages in partition", msgs)
	}
}

func testWriterMultipleTopics(t *testing.T) {
	topic1 := makeTopic()
	createTopic(t, topic1, 1)
	defer deleteTopic(t, topic1)

	offset1, err := readOffset(topic1, 0)
	if err != nil {
		t.Fatal(err)
	}

	topic2 := makeTopic()
	createTopic(t, topic2, 1)
	defer deleteTopic(t, topic2)

	offset2, err := readOffset(topic2, 0)
	if err != nil {
		t.Fatal(err)
	}

	w := newTestWriter(WriterConfig{
		Balancer: &RoundRobin{},
	})
	defer w.Close()

	msg1 := Message{Topic: topic1, Value: []byte("Hello")}
	msg2 := Message{Topic: topic2, Value: []byte("World")}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, msg1, msg2); err != nil {
		t.Error(err)
		return
	}
	ws := w.Stats()
	if ws.Writes != 2 {
		t.Error("didn't batch messages; Writes: ", ws.Writes)
		return
	}

	msgs1, err := readPartition(topic1, 0, offset1)
	if err != nil {
		t.Error("error reading partition", err)
		return
	}
	if len(msgs1) != 1 {
		t.Error("bad messages in partition", msgs1)
		return
	}
	if string(msgs1[0].Value) != "Hello" {
		t.Error("bad message in partition", msgs1)
	}

	msgs2, err := readPartition(topic2, 0, offset2)
	if err != nil {
		t.Error("error reading partition", err)
		return
	}
	if len(msgs2) != 1 {
		t.Error("bad messages in partition", msgs2)
		return
	}
	if string(msgs2[0].Value) != "World" {
		t.Error("bad message in partition", msgs2)
	}
}

func testWriterMissingTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	w := newTestWriter(WriterConfig{
		// no topic
		Balancer: &RoundRobin{},
	})
	defer w.Close()

	msg := Message{Value: []byte("Hello World")} // no topic

	if err := w.WriteMessages(ctx, msg); err == nil {
		t.Error("expected error")
		return
	}
}

func testWriterInvalidPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	w := newTestWriter(WriterConfig{
		Topic:       topic,
		MaxAttempts: 1,                              // only try once to get the error back immediately
		Balancer:    &staticBalancer{partition: -1}, // intentionally invalid partition
	})
	defer w.Close()

	msg := Message{
		Value: []byte("Hello World!"),
	}

	// this call should return an error and not panic (see issue #517)
	if err := w.WriteMessages(ctx, msg); err == nil {
		t.Fatal("expected error attempting to write message")
	}
}

func testWriterUnexpectedMessageTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	w := newTestWriter(WriterConfig{
		Topic:    topic,
		Balancer: &RoundRobin{},
	})
	defer w.Close()

	msg := Message{Topic: "should-fail", Value: []byte("Hello World")}

	if err := w.WriteMessages(ctx, msg); err == nil {
		t.Error("expected error")
		return
	}
}

func testWriteMessageWithWriterData(t *testing.T) {
	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)
	w := newTestWriter(WriterConfig{
		Topic:    topic,
		Balancer: &RoundRobin{},
	})
	defer w.Close()

	index := 0
	w.Completion = func(messages []Message, err error) {
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}

		for _, msg := range messages {
			meta := msg.WriterData.(int)
			if index != meta {
				t.Errorf("metadata is not correct, index = %d, writerData = %d", index, meta)
			}
			index += 1
		}
	}

	msg := Message{Key: []byte("key"), Value: []byte("Hello World")}
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		msg.WriterData = i
		err := w.WriteMessages(ctx, msg)
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}
	}

}

func testWriterAutoCreateTopic(t *testing.T) {
	topic := makeTopic()
	// Assume it's going to get created.
	defer deleteTopic(t, topic)

	w := newTestWriter(WriterConfig{
		Topic:    topic,
		Balancer: &RoundRobin{},
	})
	w.AllowAutoTopicCreation = true
	defer w.Close()

	msg := Message{Key: []byte("key"), Value: []byte("Hello World")}

	var err error
	const retries = 5
	for i := 0; i < retries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err = w.WriteMessages(ctx, msg)
		if errors.Is(err, LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}

		if err != nil {
			t.Errorf("unexpected error %v", err)
			return
		}
	}
	if err != nil {
		t.Errorf("unable to create topic %v", err)
	}
}

func testWriterTerminateMissingTopic(t *testing.T) {
	topic := makeTopic()

	transport := &Transport{}
	defer transport.CloseIdleConnections()

	writer := &Writer{
		Addr:                   TCP("localhost:9092"),
		Topic:                  topic,
		Balancer:               &RoundRobin{},
		RequiredAcks:           RequireNone,
		AllowAutoTopicCreation: false,
		Transport:              transport,
	}
	defer writer.Close()

	msg := Message{Value: []byte("FooBar")}

	if err := writer.WriteMessages(context.Background(), msg); err == nil {
		t.Fatal("Kafka error [3] UNKNOWN_TOPIC_OR_PARTITION is expected")
		return
	}
}

func testWriterSasl(t *testing.T) {
	topic := makeTopic()
	defer deleteTopic(t, topic)
	dialer := &Dialer{
		Timeout: 10 * time.Second,
		SASLMechanism: plain.Mechanism{
			Username: "adminplain",
			Password: "admin-secret",
		},
	}

	w := newTestWriter(WriterConfig{
		Dialer:  dialer,
		Topic:   topic,
		Brokers: []string{"localhost:9093"},
	})

	w.AllowAutoTopicCreation = true

	defer w.Close()

	msg := Message{Key: []byte("key"), Value: []byte("Hello World")}

	var err error
	const retries = 5
	for i := 0; i < retries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err = w.WriteMessages(ctx, msg)
		if errors.Is(err, LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}

		if err != nil {
			t.Errorf("unexpected error %v", err)
			return
		}
	}
	if err != nil {
		t.Errorf("unable to create topic %v", err)
	}
}

func testWriterDefaults(t *testing.T) {
	w := &Writer{}
	defer w.Close()

	if w.writeBackoffMin() != 100*time.Millisecond {
		t.Error("Incorrect default min write backoff delay")
	}

	if w.writeBackoffMax() != 1*time.Second {
		t.Error("Incorrect default max write backoff delay")
	}
}

func testWriterDefaultStats(t *testing.T) {
	w := &Writer{}
	defer w.Close()

	stats := w.Stats()

	if stats.MaxAttempts == 0 {
		t.Error("Incorrect default MaxAttempts value")
	}

	if stats.WriteBackoffMin == 0 {
		t.Error("Incorrect default WriteBackoffMin value")
	}

	if stats.WriteBackoffMax == 0 {
		t.Error("Incorrect default WriteBackoffMax value")
	}

	if stats.MaxBatchSize == 0 {
		t.Error("Incorrect default MaxBatchSize value")
	}

	if stats.BatchTimeout == 0 {
		t.Error("Incorrect default BatchTimeout value")
	}

	if stats.ReadTimeout == 0 {
		t.Error("Incorrect default ReadTimeout value")
	}

	if stats.WriteTimeout == 0 {
		t.Error("Incorrect default WriteTimeout value")
	}
}

func testWriterOverrideConfigStats(t *testing.T) {
	w := &Writer{
		MaxAttempts:     6,
		WriteBackoffMin: 2,
		WriteBackoffMax: 4,
		BatchSize:       1024,
		BatchTimeout:    16,
		ReadTimeout:     24,
		WriteTimeout:    32,
	}
	defer w.Close()

	stats := w.Stats()

	if stats.MaxAttempts != 6 {
		t.Error("Incorrect MaxAttempts value")
	}

	if stats.WriteBackoffMin != 2 {
		t.Error("Incorrect WriteBackoffMin value")
	}

	if stats.WriteBackoffMax != 4 {
		t.Error("Incorrect WriteBackoffMax value")
	}

	if stats.MaxBatchSize != 1024 {
		t.Error("Incorrect MaxBatchSize value")
	}

	if stats.BatchTimeout != 16 {
		t.Error("Incorrect BatchTimeout value")
	}

	if stats.ReadTimeout != 24 {
		t.Error("Incorrect ReadTimeout value")
	}

	if stats.WriteTimeout != 32 {
		t.Error("Incorrect WriteTimeout value")
	}
}

func testWriterNoNewPartitionWritersAfterClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	topic1 := makeTopic()
	createTopic(t, topic1, 1)
	defer deleteTopic(t, topic1)

	w := newTestWriter(WriterConfig{
		Topic: topic1,
	})
	defer w.Close() // try and close anyway after test finished

	// using balancer to close writer right between first mutex is released and second mutex is taken to make map of partition writers
	w.Balancer = mockBalancerFunc(func(m Message, i ...int) int {
		go w.Close() // close is blocking so run in goroutine
		for {        // wait until writer is marked as closed
			w.mutex.Lock()
			if w.closed {
				w.mutex.Unlock()
				break
			}
			w.mutex.Unlock()
		}
		return 0
	})

	msg := Message{Value: []byte("Hello World")} // no topic

	if err := w.WriteMessages(ctx, msg); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("expected error: %v got: %v", io.ErrClosedPipe, err)
		return
	}
}

type mockBalancerFunc func(msg Message, partitions ...int) (partition int)

func (b mockBalancerFunc) Balance(msg Message, partitions ...int) int {
	return b(msg, partitions...)
}

type staticBalancer struct {
	partition int
}

func (b *staticBalancer) Balance(_ Message, partitions ...int) int {
	return b.partition
}
