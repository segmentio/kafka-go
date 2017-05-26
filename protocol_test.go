package kafka

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"reflect"
	"testing"
)

func TestMessageCRC32(t *testing.T) {
	m := message{
		MagicByte: 1,
		Timestamp: 42,
		Key:       nil,
		Value:     []byte("Hello World!"),
	}

	b := &bytes.Buffer{}
	w := bufio.NewWriter(b)
	write(w, m)
	w.Flush()

	h := crc32.NewIEEE()
	h.Write(b.Bytes()[4:])

	sum1 := h.Sum32()
	sum2 := uint32(m.crc32())

	if sum1 != sum2 {
		t.Error("bad CRC32:")
		t.Logf("expected: %d", sum1)
		t.Logf("found:    %d", sum2)
	}
}

func TestProtocol(t *testing.T) {
	tests := []interface{}{
		requestHeader{
			Size:          26,
			ApiKey:        int16(offsetCommitRequest),
			ApiVersion:    int16(v2),
			CorrelationID: 42,
			ClientID:      "Hello World!",
		},

		message{
			MagicByte: 1,
			Timestamp: 42,
			Key:       nil,
			Value:     []byte("Hello World!"),
		},

		topicMetadataRequest{"A", "B", "C"},
		metadataResponse{
			Brokers: []broker{
				{NodeID: 1, Host: "localhost", Port: 9001},
				{NodeID: 2, Host: "localhost", Port: 9002},
			},
			Topics: []topicMetadata{
				{TopicErrorCode: 0, Partitions: []partitionMetadata{{
					PartitionErrorCode: 0,
					PartitionID:        1,
					Leader:             2,
					Replicas:           []int32{1},
					Isr:                []int32{1},
				}}},
			},
		},

		listOffsetRequest{
			ReplicaID: 1,
			Topics: []listOffsetRequestTopic{
				{TopicName: "A", Partitions: []listOffsetRequestPartition{
					{Partition: 0, Time: -1},
					{Partition: 1, Time: -1},
					{Partition: 2, Time: -1},
				}},
				{TopicName: "B", Partitions: []listOffsetRequestPartition{
					{Partition: 0, Time: -2},
				}},
				{TopicName: "C", Partitions: []listOffsetRequestPartition{
					{Partition: 0, Time: 42},
				}},
			},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%T", test), func(t *testing.T) {
			b := &bytes.Buffer{}
			r := bufio.NewReader(b)
			w := bufio.NewWriter(b)

			if err := write(w, test); err != nil {
				t.Fatal(err)
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}

			if size := int(sizeof(test)); size != b.Len() {
				t.Error("invalid size:", size, "!=", b.Len())
			}

			v := reflect.New(reflect.TypeOf(test))

			if err := read(r, v.Interface()); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(test, v.Elem().Interface()) {
				t.Error("values don't match:")
				t.Logf("expected: %#v", test)
				t.Logf("found:    %#v", v.Elem().Interface())
			}
		})
	}
}
