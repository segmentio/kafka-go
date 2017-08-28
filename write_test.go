package kafka

import (
	"bufio"
	"bytes"
	"testing"
	"time"
)

const (
	testCorrelationID = 1
	testClientID      = "localhost"
	testTopic         = "topic"
	testPartition     = 42
)

func TestWriteOptimizations(t *testing.T) {
	t.Parallel()
	t.Run("writeFetchRequestV1", testWriteFetchRequestV1)
	t.Run("writeListOffsetRequestV1", testWriteListOffsetRequestV1)
	t.Run("writeProduceRequestV2", testWriteProduceRequestV2)
}

func testWriteFetchRequestV1(t *testing.T) {
	const offset = 42
	const minBytes = 10
	const maxBytes = 1000
	const maxWait = 100 * time.Millisecond
	testWriteOptimization(t,
		requestHeader{
			ApiKey:        int16(fetchRequest),
			ApiVersion:    int16(v1),
			CorrelationID: testCorrelationID,
			ClientID:      testClientID,
		},
		fetchRequestV1{
			ReplicaID:   -1,
			MaxWaitTime: milliseconds(maxWait),
			MinBytes:    minBytes,
			Topics: []fetchRequestTopicV1{{
				TopicName: testTopic,
				Partitions: []fetchRequestPartitionV1{{
					Partition:   testPartition,
					FetchOffset: offset,
					MaxBytes:    maxBytes,
				}},
			}},
		},
		func(w *bufio.Writer) {
			writeFetchRequestV1(w, testCorrelationID, testClientID, testTopic, testPartition, offset, minBytes, maxBytes, maxWait)
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
					MessageSetSize: msg.size(),
					MessageSet:     messageSet{msg},
				}},
			}},
		},
		func(w *bufio.Writer) {
			writeProduceRequestV2(w, testCorrelationID, testClientID, testTopic, testPartition, timeout*time.Millisecond, -1, Message{
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
