package kafka

import (
	"context"
	"errors"
	"io"
	"strings"
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
			scenario: "writing 1 message through a writer using round-robin balancing produces 1 message to the first partition",
			function: testWriterRoundRobin1,
		},

		{
			scenario: "running out of max attempts should return an error",
			function: testWriterMaxAttemptsErr,
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
	w := newTestWriter(WriterConfig{
		Topic: topic,
	})

	if err := w.Close(); err != nil {
		t.Error(err)
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

type fakeWriter struct{}

func (f *fakeWriter) messages() chan<- writerMessage {
	ch := make(chan writerMessage, 1)

	go func() {
		for {
			msg := <-ch
			msg.res <- &writerError{
				err: errors.New("bad attempt"),
			}
		}
	}()

	return ch
}

func (f *fakeWriter) close() {

}

func testWriterMaxAttemptsErr(t *testing.T) {
	const topic = "test-writer-1"

	createTopic(t, topic, 1)
	w := newTestWriter(WriterConfig{
		Topic:       topic,
		MaxAttempts: 1,
		Balancer:    &RoundRobin{},
		newPartitionWriter: func(p int, config WriterConfig, stats *writerStats) partitionWriter {
			return &fakeWriter{}
		},
	})
	defer w.Close()

	if err := w.WriteMessages(context.Background(), Message{
		Value: []byte("Hello World!"),
	}); err == nil {
		t.Error("expected error")
		return
	} else if err != nil {
		if !strings.Contains(err.Error(), "bad attempt") {
			t.Errorf("unexpected error: %s", err)
			return
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
