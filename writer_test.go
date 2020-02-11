package kafka

import (
	"context"
	"errors"
	"io"
	"math"
	"testing"
	"time"
)

func TestWriter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			scenario: "closing a writer right after creating it returns promptly with no error",
			function: testWriterClose,
		},
		{
			scenario: "writing messages on closed writer should return error",
			function: testClosedWriterErr,
		},
		{
			scenario: "writing empty Message slice returns promptly with no error",
			function: testEmptyWrite,
		},
		{
			scenario: "writing messages after context is done should return an error",
			function: testContextDoneErr,
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
			scenario: "writing messsages with a small batch byte size",
			function: testWriterSmallBatchBytes,
		},
		{
			scenario: "writing messages with retries enabled",
			function: testWriterRetries,
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

type writerTestCase WriterErrors

func (wt writerTestCase) errorsEqual(wes WriterErrors) bool {
	exp := make(map[string]int)
	numExp := 0
	for _, t := range wt {
		if t.Err != nil {
			numExp += 1
			k := string(t.Msg.Value) + t.Err.Error()
			if _, ok := exp[k]; ok {
				exp[k] += 1
			} else {
				exp[k] = 1
			}
		}
	}

	if len(wes) != numExp {
		return false
	}

	for _, e := range wes {
		k := string(e.Msg.Value) + e.Err.Error()
		if _, ok := exp[k]; ok {
			exp[k] -= 1
		} else {
			return false
		}
	}

	for _, e := range exp {
		if e != 0 {
			return false
		}
	}

	return true
}

func (wt writerTestCase) msgs() []Message {
	msgs := make([]Message, len(wt))
	for i, m := range wt {
		msgs[i] = m.Msg
	}

	return msgs
}

func (wt writerTestCase) expected() WriterErrors {
	exp := make(WriterErrors, 0, len(wt))
	for _, v := range wt {
		if v.Err != nil {
			exp = append(exp, v)
		}
	}

	if len(exp) > 0 {
		return exp
	}

	return nil
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
	w := newTestWriter(WriterConfig{
		Topic: topic,
	})

	if err := w.Close(); err != nil {
		t.Error(err)
	}
}

func testClosedWriterErr(t *testing.T) {
	tcs := []writerTestCase{
		{
			{
				Msg: Message{Value: []byte("Hello World!")},
				Err: io.ErrClosedPipe,
			},
		},
		{
			{
				Msg: Message{Value: []byte("Hello")},
				Err: io.ErrClosedPipe,
			},
			{
				Msg: Message{Value: []byte("World!")},
				Err: io.ErrClosedPipe,
			},
		},
	}

	const topic = "test-writer-0"
	w := newTestWriter(WriterConfig{
		Topic: topic,
	})

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	for i, tc := range tcs {
		err := w.WriteMessages(context.Background(), tc.msgs()...)
		if err == nil {
			t.Errorf("test %d: expected error", i)
			continue
		}

		wes, ok := err.(WriterErrors)
		if !ok {
			t.Errorf("test %d: expected WriterErrors", i)
			continue
		}

		if !tc.errorsEqual(wes) {
			t.Errorf("test %d: unexpected errors occurred.\nExpected:\n%sFound:\n%s", i, tc.expected(), wes)
		}
	}
}

func testEmptyWrite(t *testing.T) {
	const topic = "test-writer-0"
	w := newTestWriter(WriterConfig{
		Topic: topic,
	})

	defer func() {
		_ = w.Close()
	}()

	if err := w.WriteMessages(context.Background(), []Message{}...); err != nil {
		t.Error("unexpected error occurred", err)
	}
}

func testContextDoneErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tcs := []writerTestCase{
		{
			{
				Msg: Message{Value: []byte("Hello World!")},
				Err: ctx.Err(),
			},
		},
		{
			{
				Msg: Message{Value: []byte("Hello")},
				Err: ctx.Err(),
			},
			{
				Msg: Message{Value: []byte("World")},
				Err: ctx.Err(),
			},
		},
	}

	const topic = "test-writer-0"
	w := newTestWriter(WriterConfig{
		Topic: topic,
	})

	defer func() {
		_ = w.Close()
	}()

	for i, tc := range tcs {
		err := w.WriteMessages(ctx, tc.msgs()...)
		if err == nil {
			t.Errorf("test %d: expected error", i)
			continue
		}

		wes, ok := err.(WriterErrors)
		if !ok {
			t.Errorf("test %d: expected WriterErrors", i)
			continue
		}

		if !tc.errorsEqual(wes) {
			t.Errorf("test %d: unexpected errors occurred.\nExpected:\n%sFound:\n%s", i, tc.expected(), wes)
		}
	}
}

func testWriterRoundRobin1(t *testing.T) {
	const topic = "test-writer-1"

	createTopic(t, topic, 1)
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
		{config: WriterConfig{Brokers: []string{"broker1", "broker2"}}, errorOccured: true},
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

type errorWriter struct{}

func (w *errorWriter) messages() chan<- writerMessage {
	ch := make(chan writerMessage, 1)

	go func() {
		for {
			msg := <-ch
			msg.res <- writerResponse{
				id: msg.id,
				err: &WriterError{
					Err: errors.New("bad attempt"),
					Msg: msg.msg,
				},
			}
		}
	}()

	return ch
}

func (w *errorWriter) close() {

}

func testWriterMaxAttemptsErr(t *testing.T) {
	tcs := []writerTestCase{
		{
			{
				Msg: Message{Value: []byte("test 1 error")},
				Err: errors.New("bad attempt"),
			},
		},
		{
			{
				Msg: Message{Value: []byte("test multi error")},
				Err: errors.New("bad attempt"),
			},
			{
				Msg: Message{Value: []byte("test multi error")},
				Err: errors.New("bad attempt"),
			},
		},
	}

	const topic = "test-writer-2"

	createTopic(t, topic, 1)
	w := newTestWriter(WriterConfig{
		Topic:       topic,
		MaxAttempts: 2,
		Balancer:    &RoundRobin{},
		newPartitionWriter: func(_ int, _ WriterConfig, _ *writerStats) partitionWriter {
			return &errorWriter{}
		},
	})
	defer func() {
		_ = w.Close()
	}()

	for i, tc := range tcs {
		err := w.WriteMessages(context.Background(), tc.msgs()...)
		if err == nil {
			t.Errorf("test %d: expected error", i)
			continue
		}

		wes, ok := err.(WriterErrors)
		if !ok {
			t.Errorf("test %d: expected WriterErrors", i)
			continue
		}

		if !tc.errorsEqual(wes) {
			t.Errorf("test %d: unexpected errors occurred.\nExpected:\n%sFound:\n%s", i, tc.expected(), wes)
		}
	}
}

func testWriterMaxBytes(t *testing.T) {
	tcs := []writerTestCase{
		{
			{
				Msg: Message{Value: []byte("Hello World!")},
				Err: MessageTooLargeError{},
			},
			{
				Msg: Message{Value: []byte("Hi")},
				Err: nil,
			},
		},
		{
			{
				Msg: Message{Value: []byte("Too large!")},
				Err: MessageTooLargeError{},
			},
			{
				Msg: Message{Value: []byte("Also too long!")},
				Err: MessageTooLargeError{},
			},
		},
	}

	topic := makeTopic()
	maxBytes := 25
	createTopic(t, topic, 1)
	w := newTestWriter(WriterConfig{
		Topic:      topic,
		BatchBytes: maxBytes,
	})

	defer func() {
		_ = w.Close()
	}()

	if err := w.WriteMessages(context.Background(), Message{
		Value: []byte("Hi"),
	}); err != nil {
		t.Error(err)
		return
	}

	for i, tc := range tcs {
		err := w.WriteMessages(context.Background(), tc.msgs()...)
		if err == nil {
			t.Errorf("test %d: expected error", i)
			continue
		}

		wes, ok := err.(WriterErrors)
		if !ok {
			t.Errorf("test %d: expected WriterErrors", i)
			continue
		}

		if !tc.errorsEqual(wes) {
			t.Errorf("test %d: unexpected errors occurred.\nExpected:\n%sFound:\n%s", i, tc.expected(), wes)
		}
	}
}

func readOffset(topic string, partition int) (offset int64, err error) {
	var conn *Conn

	if conn, err = DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition); err != nil {
		return
	}
	defer conn.Close()

	offset, err = conn.ReadLastOffset()
	return
}

func readPartition(topic string, partition int, offset int64) (msgs []Message, err error) {
	var conn *Conn

	if conn, err = DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition); err != nil {
		return
	}
	defer conn.Close()

	conn.Seek(offset, SeekAbsolute)
	conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	batch := conn.ReadBatch(0, 1000000000)
	defer batch.Close()

	for {
		var msg Message

		if msg, err = batch.ReadMessage(); err != nil {
			if err == io.EOF {
				err = nil
			}
			return
		}

		msgs = append(msgs, msg)
	}
}

func testWriterBatchBytes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const topic = "test-writer-1-bytes"

	createTopic(t, topic, 1)
	offset, err := readOffset(topic, 0)
	if err != nil {
		t.Fatal(err)
	}

	w := newTestWriter(WriterConfig{
		Topic:        topic,
		BatchBytes:   48,
		BatchTimeout: math.MaxInt32 * time.Second,
		Balancer:     &RoundRobin{},
	})
	defer w.Close()

	if err := w.WriteMessages(ctx, []Message{
		Message{Value: []byte("Hi")}, // 24 Bytes
		Message{Value: []byte("By")}, // 24 Bytes
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

func testWriterBatchSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := makeTopic()
	createTopic(t, topic, 1)
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

	if err := w.WriteMessages(ctx, []Message{
		Message{Value: []byte("Hi")}, // 24 Bytes
		Message{Value: []byte("By")}, // 24 Bytes
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := makeTopic()
	createTopic(t, topic, 1)
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

	if err := w.WriteMessages(ctx, []Message{
		Message{Value: []byte("Hi")}, // 24 Bytes
		Message{Value: []byte("By")}, // 24 Bytes
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

type testRetryWriter struct {
	errs int
}

func (w *testRetryWriter) messages() chan<- writerMessage {
	ch := make(chan writerMessage, 1)

	go func() {
		for {
			msg := <-ch
			if w.errs > 0 {
				msg.res <- writerResponse{
					id: msg.id,
					err: &WriterError{
						Msg: msg.msg,
						Err: errors.New("bad attempt"),
					},
				}
				w.errs -= 1
			} else {
				msg.res <- writerResponse{
					id:  msg.id,
					err: nil,
				}
			}
		}
	}()

	return ch
}

func (w *testRetryWriter) close() {

}

func testWriterRetries(t *testing.T) {
	tcs := []writerTestCase{
		{
			{
				Msg: Message{Value: []byte("test message 1")},
				Err: nil,
			},
			{
				Msg: Message{Value: []byte("test message 2")},
				Err: nil,
			},
		},
		{
			{
				Msg: Message{Value: []byte("these messages")},
				Err: nil,
			},
			{
				Msg: Message{Value: []byte("should succeed")},
				Err: nil,
			},
			{
				Msg: Message{Value: []byte("for this test case")},
				Err: nil,
			},
		},
		{
			{
				Msg: Message{Value: []byte("this message should fail")},
				Err: errors.New("bad attempt"),
			},
		},
	}

	const topic = "test-writer-retry"
	createTopic(t, topic, 1)

	for i, tc := range tcs {
		w := newTestWriter(WriterConfig{
			Topic:       topic,
			MaxAttempts: 2,
			Balancer:    &RoundRobin{},
			newPartitionWriter: func(_ int, _ WriterConfig, _ *writerStats) partitionWriter {
				return &testRetryWriter{errs: 2}
			},
		})

		err := w.WriteMessages(context.Background(), tc.msgs()...)
		if err == nil {
			if tc.expected() != nil {
				t.Errorf("test %d: expected error", i)
			}
		} else {
			if wes, ok := err.(WriterErrors); !ok {
				t.Errorf("test %d: expected WriterErrors", i)
			} else {
				if !tc.errorsEqual(wes) {
					t.Errorf("test %d: unexpected errors occurred.\nExpected:\n%sFound:\n%s", i, tc.expected(), wes)
				}
			}
		}

		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
