package kafka

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	ktesting "github.com/segmentio/kafka-go/testing"
)

const (
	testCorrelationID = 1
	testClientID      = "localhost"
	testTopic         = "topic"
	testPartition     = 42
)

type WriteVarIntTestCase struct {
	v  []byte
	tc int64
}

func TestWriteVarInt(t *testing.T) {
	testCases := []*WriteVarIntTestCase{
		&WriteVarIntTestCase{v: []byte{0}, tc: 0},
		&WriteVarIntTestCase{v: []byte{2}, tc: 1},
		&WriteVarIntTestCase{v: []byte{1}, tc: -1},
		&WriteVarIntTestCase{v: []byte{3}, tc: -2},
		&WriteVarIntTestCase{v: []byte{128, 2}, tc: 128},
		&WriteVarIntTestCase{v: []byte{254, 1}, tc: 127},
		&WriteVarIntTestCase{v: []byte{142, 6}, tc: 391},
		&WriteVarIntTestCase{v: []byte{142, 134, 6}, tc: 49543},
	}

	for _, tc := range testCases {
		buf := &bytes.Buffer{}
		bufWriter := bufio.NewWriter(buf)
		writeVarInt(bufWriter, tc.tc)
		bufWriter.Flush()
		if !bytes.Equal(buf.Bytes(), tc.v) {
			t.Errorf("Expected %v; got %v", tc.v, buf.Bytes())
		}
	}
}

func TestWriteOptimizations(t *testing.T) {
	t.Parallel()
	t.Run("writeFetchRequestV2", testWriteFetchRequestV2)
	t.Run("writeListOffsetRequestV1", testWriteListOffsetRequestV1)
	t.Run("writeProduceRequestV2", testWriteProduceRequestV2)
}

func testWriteFetchRequestV2(t *testing.T) {
	const offset = 42
	const minBytes = 10
	const maxBytes = 1000
	const maxWait = 100 * time.Millisecond
	testWriteOptimization(t,
		requestHeader{
			ApiKey:        int16(fetchRequest),
			ApiVersion:    int16(v2),
			CorrelationID: testCorrelationID,
			ClientID:      testClientID,
		},
		fetchRequestV2{
			ReplicaID:   -1,
			MaxWaitTime: milliseconds(maxWait),
			MinBytes:    minBytes,
			Topics: []fetchRequestTopicV2{{
				TopicName: testTopic,
				Partitions: []fetchRequestPartitionV2{{
					Partition:   testPartition,
					FetchOffset: offset,
					MaxBytes:    maxBytes,
				}},
			}},
		},
		func(w *bufio.Writer) {
			writeFetchRequestV2(w, testCorrelationID, testClientID, testTopic, testPartition, offset, minBytes, maxBytes, maxWait)
		},
	)
}

func testWriteListOffsetRequestV1(t *testing.T) {
	const time = -1
	testWriteOptimization(t,
		requestHeader{
			ApiKey:        int16(listOffsetRequest),
			ApiVersion:    int16(v1),
			CorrelationID: testCorrelationID,
			ClientID:      testClientID,
		},
		listOffsetRequestV1{
			ReplicaID: -1,
			Topics: []listOffsetRequestTopicV1{{
				TopicName: testTopic,
				Partitions: []listOffsetRequestPartitionV1{{
					Partition: testPartition,
					Time:      time,
				}},
			}},
		},
		func(w *bufio.Writer) {
			writeListOffsetRequestV1(w, testCorrelationID, testClientID, testTopic, testPartition, time)
		},
	)
}

func testWriteProduceRequestV2(t *testing.T) {
	key := []byte(nil)
	val := []byte("Hello World!")

	msg := messageSetItem{
		Offset: 10,
		Message: message{
			MagicByte:  1,
			Attributes: 0,
			Key:        key,
			Value:      val,
		},
	}
	msg.MessageSize = msg.Message.size()
	msg.Message.CRC = msg.Message.crc32()

	const timeout = 100
	testWriteOptimization(t,
		requestHeader{
			ApiKey:        int16(produceRequest),
			ApiVersion:    int16(v2),
			CorrelationID: testCorrelationID,
			ClientID:      testClientID,
		},
		produceRequestV2{
			RequiredAcks: -1,
			Timeout:      timeout,
			Topics: []produceRequestTopicV2{{
				TopicName: testTopic,
				Partitions: []produceRequestPartitionV2{{
					Partition:      testPartition,
					MessageSetSize: msg.size(), MessageSet: messageSet{msg},
				}},
			}},
		},
		func(w *bufio.Writer) {
			writeProduceRequestV2(w, nil, testCorrelationID, testClientID, testTopic, testPartition, timeout*time.Millisecond, -1, Message{
				Offset: 10,
				Key:    key,
				Value:  val,
			})
		},
	)
}

func testWriteOptimization(t *testing.T, h requestHeader, r request, f func(*bufio.Writer)) {
	b1 := &bytes.Buffer{}
	w1 := bufio.NewWriter(b1)

	b2 := &bytes.Buffer{}
	w2 := bufio.NewWriter(b2)

	h.Size = (h.size() + r.size()) - 4
	h.writeTo(w1)
	r.writeTo(w1)
	w1.Flush()

	f(w2)
	w2.Flush()

	c1 := b1.Bytes()
	c2 := b2.Bytes()

	if !bytes.Equal(c1, c2) {
		t.Error("content differs")

		n1 := len(c1)
		n2 := len(c2)

		if n1 != n2 {
			t.Log("content length 1 =", n1)
			t.Log("content length 2 =", n2)
		} else {
			for i := 0; i != n1; i++ {
				if c1[i] != c2[i] {
					t.Logf("byte at offset %d/%d: %#x != %#x", i, n1, c1[i], c2[i])
					break
				}
			}
		}
	}
}

func TestWriteV2RecordBatch(t *testing.T) {

	if !ktesting.KafkaIsAtLeast("0.11.0") {
		t.Skip("RecordBatch was added in kafka 0.11.0")
		return
	}

	topic := CreateTopic(t, 1)
	msgs := make([]Message, 15)
	for i := range msgs {
		value := fmt.Sprintf("Sample message content: %d!", i)
		msgs[i] = Message{Key: []byte("Key"), Value: []byte(value), Headers: []Header{Header{Key: "hk", Value: []byte("hv")}}}
	}
	w := NewWriter(WriterConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        topic,
		BatchTimeout: 100 * time.Millisecond,
		BatchSize:    5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := w.WriteMessages(ctx, msgs...); err != nil {
		t.Errorf("Failed to write v2 messages to kafka: %v", err)
		return
	}
	w.Close()

	r := NewReader(ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   topic,
		MaxWait: 100 * time.Millisecond,
	})
	defer r.Close()

	msg, err := r.ReadMessage(context.Background())
	if err != nil {
		t.Error("Failed to read message")
		return
	}

	if string(msg.Key) != "Key" {
		t.Error("Received message's key doesn't match")
		return
	}
	if msg.Headers[0].Key != "hk" {
		t.Error("Received message header's key doesn't match")
		return
	}
}
