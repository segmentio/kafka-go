package kafka

import (
	"bufio"
	"bytes"
	"testing"
	"time"
)

// This test ensures that the writeProduceRequest function (which exists for
// optimization purposes) and the generic produceRequestV2.writeTo method behave
// the same.
func TestWriteProduceRequest(t *testing.T) {
	b1 := &bytes.Buffer{}
	w1 := bufio.NewWriter(b1)

	b2 := &bytes.Buffer{}
	w2 := bufio.NewWriter(b2)

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

	req := produceRequestV2{
		RequiredAcks: -1,
		Timeout:      100,
		Topics: []produceRequestTopicV2{{
			TopicName: "A",
			Partitions: []produceRequestPartitionV2{{
				Partition:      42,
				MessageSetSize: msg.size(),
				MessageSet:     messageSet{msg},
			}},
		}},
	}

	hdr := requestHeader{
		ApiKey:        int16(produceRequest),
		ApiVersion:    int16(v2),
		CorrelationID: 1,
		ClientID:      "localhost",
	}
	hdr.Size = (hdr.size() + req.size()) - 4
	hdr.writeTo(w1)
	req.writeTo(w1)
	w1.Flush()

	writeProduceRequest(w2, 1, "localhost", "A", 42, 100*time.Millisecond, Message{
		Offset: 10,
		Key:    key,
		Value:  val,
	})

	c1 := b1.Bytes()
	c2 := b2.Bytes()

	if !bytes.Equal(c1, c2) {
		t.Error("content differs")
		t.Log("content length 1 =", len(c1))
		t.Log("content length 2 =", len(c2))
	}
}
