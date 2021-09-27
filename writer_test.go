package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"
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
	go func() {
		wg.Add(1)
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

// TestWriter tests behavior of the Writer interface against two different
// ways of instantiation:
// - the NewWriter constructor (deprecated, but still in use, so we should
//   continue to test it)
// - the Writer{} struct literal
func TestWriter(t *testing.T) {
	tests := []struct {

		// description to be used for subtest name
		scenario string

		// write your test of Writer behavior in this function
		function func(*testing.T, *Writer)

		// the literal Writer - used directly for literal tests and converted to
		// WriterConfig for constructor tests
		writer *Writer

		// set the topic on the Writer
		setTopic bool

		// create the topic in Kafka
		createTopic bool

		// number of partitions on the created topic. Defaults to 1
		partitions int
	}{
		{
			scenario:    "closing a writer right after creating it returns promptly with no error",
			function:    testWriterClose,
			setTopic:    true,
			createTopic: true,
			writer:      &Writer{},
		},
		{
			scenario:    "writing 1 message through a writer using round-robin balancing produces 1 message to the first partition",
			function:    testWriterRoundRobin1,
			setTopic:    true,
			createTopic: true,
			writer: &Writer{
				Balancer: &RoundRobin{},
			},
		},
		{
			scenario:    "running out of max attempts should return an error",
			function:    testWriterMaxAttemptsErr,
			setTopic:    true,
			createTopic: true,
			writer: &Writer{
				Addr:        TCP("localhost:9999"),
				Balancer:    &RoundRobin{},
				MaxAttempts: 3,
			},
		},
		{
			scenario:    "writing a message larger then the max bytes should return an error",
			function:    testWriterMaxBytes,
			setTopic:    true,
			createTopic: true,
			writer: &Writer{
				BatchBytes: 25,
			},
		},
		{
			scenario:    "writing a batch of message based on batch byte size",
			function:    testWriterBatchBytes,
			setTopic:    true,
			createTopic: true,
			writer: &Writer{
				BatchBytes:   48,
				BatchTimeout: math.MaxInt32 * time.Second,
				Balancer:     &RoundRobin{},
			},
		},
		{
			scenario:    "writing a batch of messages",
			function:    testWriterBatchSize,
			setTopic:    true,
			createTopic: true,
			writer: &Writer{
				BatchSize:    2,
				BatchTimeout: math.MaxInt32 * time.Second,
				Balancer:     &RoundRobin{},
			},
		},
		{
			scenario:    "writing messages with a small batch byte size",
			function:    testWriterSmallBatchBytes,
			setTopic:    true,
			createTopic: true,
			writer: &Writer{
				BatchBytes:   25,
				BatchTimeout: 50 * time.Millisecond,
				Balancer:     &RoundRobin{},
			},
		},
		{
			scenario: "writing messages to multiple topics",
			function: testWriterMultipleTopics,
			writer: &Writer{
				Balancer: &RoundRobin{},
			},
		},
		{
			scenario: "writing messages without specifying a topic",
			function: testWriterMissingTopic,
			writer: &Writer{
				Balancer: &RoundRobin{},
			},
		},
		{
			scenario:    "specifying topic for message when already set for writer",
			function:    testWriterUnexpectedMessageTopic,
			setTopic:    true,
			createTopic: true,
			writer: &Writer{
				Balancer: &RoundRobin{},
			},
		},
		{
			scenario:    "writing a message to an invalid partition",
			function:    testWriterInvalidPartition,
			setTopic:    true,
			createTopic: true,
			writer: &Writer{
				Balancer:    &staticBalancer{partition: -1},
				MaxAttempts: 1,
			},
		},
	}

	for _, test := range tests {
		testFunc := test.function

		// default to 1 partition
		if test.partitions == 0 {
			test.partitions = 1
		}

		// client for making topics if necessary
		client := &Client{}

		if test.writer == nil || test.writer.Addr == nil {
			client.Addr = TCP("localhost:9092")
		} else {
			client.Addr = test.writer.Addr
		}

		t.Run(test.scenario, func(t *testing.T) {
			// run test against deprecated constructor style
			t.Run("constructor", func(t *testing.T) {
				t.Parallel()

				cfg := WriterConfig{
					Balancer:     test.writer.Balancer,
					MaxAttempts:  test.writer.MaxAttempts,
					BatchBytes:   int(test.writer.BatchBytes),
					BatchSize:    test.writer.BatchSize,
					BatchTimeout: test.writer.BatchTimeout,
					Logger:       &testKafkaLogger{T: t},
				}

				if test.setTopic {
					cfg.Topic = makeTestTopic(t)
				}
				if test.createTopic {
					err := clientCreateTopic(client, cfg.Topic, test.partitions)
					if err != nil {
						t.Fatal(err.Error())
					}
					t.Cleanup(func() {
						deleteTopic(t, cfg.Topic)
					})
				}
				if test.writer.Addr != nil {
					cfg.Brokers = []string{test.writer.Addr.String()}
				}
				w := newTestWriter(cfg)
				t.Cleanup(func() {
					err := w.Close()
					if err != nil {
						t.Fatal(err)
					}
				})
				testFunc(t, w)
			})

			// run test against struct literal style
			t.Run("literal", func(t *testing.T) {
				t.Parallel()

				w := test.writer
				w.Logger = &testKafkaLogger{T: t}

				if test.setTopic {
					w.Topic = makeTestTopic(t)
				}
				if test.createTopic {
					err := clientCreateTopic(client, w.Topic, test.partitions)
					if err != nil {
						t.Fatal(err.Error())
					}
					t.Cleanup(func() {
						deleteTopic(t, w.Topic)
					})
				}
				if w.Addr == nil {
					w.Addr = TCP("localhost:9092")
				}
				t.Cleanup(func() {
					err := w.Close()
					if err != nil {
						t.Fatal(err)
					}
				})
				testFunc(t, w)
			})
		})
	}
}

func newTestWriter(config WriterConfig) *Writer {
	if len(config.Brokers) == 0 {
		config.Brokers = []string{"localhost:9092"}
	}
	return NewWriter(config)
}

func testWriterClose(t *testing.T, w *Writer) {
	if err := w.Close(); err != nil {
		t.Error(err)
	}
}

// Converting to standalone test since constructor does not support Transport
// configuration
func TestWriterRequiredAcksNone(t *testing.T) {
	topic := makeTestTopic(t)
	createTopic(t, topic, 1)

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := writer.WriteMessages(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
}

// Moving this test to be a standalone test since it only applies to the
// constructor, not the literal construction
func TestWriterSetsRightBalancer(t *testing.T) {
	t.Log(`setting a non default balancer on the writer`)

	balancer := &CRC32Balancer{}
	w := newTestWriter(WriterConfig{
		Balancer: balancer,
	})
	defer w.Close()

	if w.Balancer != balancer {
		t.Errorf("Balancer not set correctly")
	}
}

func testWriterRoundRobin1(t *testing.T, w *Writer) {
	topic := w.Topic
	offset, err := readOffset(topic, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.WriteMessages(context.Background(), Message{
		Value: []byte("Hello World!"),
	}); err != nil {
		t.Error(err.Error())
		return
	}

	msgs, err := readPartition(topic, 0, offset)
	if err != nil {
		w.Logger.Printf("error reading partition: %s", err.Error())
		t.Fail()
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
		config        WriterConfig
		errorOccurred bool
	}{
		{config: WriterConfig{}, errorOccurred: true},
		{
			config: WriterConfig{
				Brokers: []string{
					"broker1",
					"broker2",
				},
			},
			errorOccurred: false,
		},
		{
			config: WriterConfig{
				Brokers: []string{"broker1"},
				Topic:   "topic1",
			},
			errorOccurred: false,
		},
	}
	for _, test := range tests {
		err := test.config.Validate()
		if test.errorOccurred && err == nil {
			t.Fail()
		}
		if !test.errorOccurred && err != nil {
			t.Fail()
		}
	}
}

func testWriterMaxAttemptsErr(t *testing.T, w *Writer) {
	if err := w.WriteMessages(context.Background(), Message{
		Value: []byte("Hello World!"),
	}); err == nil {
		t.Error("expected error")
		return
	}
}

func testWriterMaxBytes(t *testing.T, w *Writer) {
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
		switch e := err.(type) {
		case MessageTooLargeError:
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
		err = fmt.Errorf("readPartition: error in DialLeader: %w", err)
		return
	}
	defer conn.Close()

	conn.Seek(offset, SeekAbsolute)
	batch := conn.ReadBatch(0, 1000000000)
	defer batch.Close()

	for {
		var msg Message

		if msg, err = batch.ReadMessage(); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			if err != nil {
				err = fmt.Errorf("readPartition: error in ReadMessage: %w", err)
			}
			return
		}

		msgs = append(msgs, msg)
	}
}

func testWriterBatchBytes(t *testing.T, w *Writer) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := w.Topic
	offset, err := readOffset(topic, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.WriteMessages(ctx, []Message{
		{Value: []byte("M0")}, // 24 Bytes
		{Value: []byte("M1")}, // 24 Bytes
		{Value: []byte("M2")}, // 24 Bytes
		{Value: []byte("M3")}, // 24 Bytes
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

	t.Logf("number of messages: %d", len(msgs))
	if len(msgs) != 4 {
		t.Error("bad messages in partition", msgs)

		for _, m := range msgs {
			t.Logf("message offset=%d key=%q value=%q", m.Offset,
				string(m.Key), string(m.Value))
		}
		return
	}

	for i, m := range msgs {
		if string(m.Value) == "M"+strconv.Itoa(i) {
			continue
		}
		t.Error("bad messages in partition", string(m.Value))
	}
}

func testWriterBatchSize(t *testing.T, w *Writer) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := w.Topic
	offset, err := readOffset(topic, 0)
	if err != nil {
		t.Fatal(err)
	}

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

func testWriterSmallBatchBytes(t *testing.T, w *Writer) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := w.Topic

	offset, err := readOffset(topic, 0)
	if err != nil {
		t.Fatal(err)
	}

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

func testWriterMultipleTopics(t *testing.T, w *Writer) {
	topic1 := makeTestTopic(t)
	createTopic(t, topic1, 1)
	t.Cleanup(func() {
		deleteTopic(t, topic1)
	})

	offset1, err := readOffset(topic1, 0)
	if err != nil {
		t.Fatal(err)
	}

	topic2 := makeTestTopic(t)
	createTopic(t, topic2, 1)
	t.Cleanup(func() {
		deleteTopic(t, topic2)
	})

	offset2, err := readOffset(topic2, 0)
	if err != nil {
		t.Fatal(err)
	}

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

func testWriterMissingTopic(t *testing.T, w *Writer) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg := Message{Value: []byte("Hello World")} // no topic

	if err := w.WriteMessages(ctx, msg); err == nil {
		t.Error("expected error")
		return
	}
}

func testWriterInvalidPartition(t *testing.T, w *Writer) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg := Message{
		Value: []byte("Hello World!"),
	}

	// this call should return an error and not panic (see issue #517)
	if err := w.WriteMessages(ctx, msg); err == nil {
		t.Fatal("expected error attempting to write message")
	}
}

func testWriterUnexpectedMessageTopic(t *testing.T, w *Writer) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg := Message{Topic: "should-fail", Value: []byte("Hello World")}

	if err := w.WriteMessages(ctx, msg); err == nil {
		t.Error("expected error")
		return
	}
}

type staticBalancer struct {
	partition int
}

func (b *staticBalancer) Balance(Message, ...int) int {
	return b.partition
}
