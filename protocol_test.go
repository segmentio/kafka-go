package kafka

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

func TestProtocol(t *testing.T) {
	tests := []interface{}{
		int8(42),
		int16(42),
		int32(42),
		int64(42),
		"",
		"Hello World!",
		[]byte(nil),
		[]byte("Hello World!"),

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

		topicMetadataRequestV1{"A", "B", "C"},

		metadataResponseV1{
			Brokers: []brokerMetadataV1{
				{NodeID: 1, Host: "localhost", Port: 9001},
				{NodeID: 2, Host: "localhost", Port: 9002, Rack: "rack2"},
			},
			ControllerID: 2,
			Topics: []topicMetadataV1{
				{TopicErrorCode: 0, Internal: true, Partitions: []partitionMetadataV1{{
					PartitionErrorCode: 0,
					PartitionID:        1,
					Leader:             2,
					Replicas:           []int32{1},
					Isr:                []int32{1},
				}}},
			},
		},

		listOffsetRequestV1{
			ReplicaID: 1,
			Topics: []listOffsetRequestTopicV1{
				{TopicName: "A", Partitions: []listOffsetRequestPartitionV1{
					{Partition: 0, Time: -1},
					{Partition: 1, Time: -1},
					{Partition: 2, Time: -1},
				}},
				{TopicName: "B", Partitions: []listOffsetRequestPartitionV1{
					{Partition: 0, Time: -2},
				}},
				{TopicName: "C", Partitions: []listOffsetRequestPartitionV1{
					{Partition: 0, Time: 42},
				}},
			},
		},

		listOffsetResponseV1{
			{TopicName: "A", PartitionOffsets: []partitionOffsetV1{
				{Partition: 0, Timestamp: 42, Offset: 1},
			}},
			{TopicName: "B", PartitionOffsets: []partitionOffsetV1{
				{Partition: 0, Timestamp: 43, Offset: 10},
				{Partition: 1, Timestamp: 44, Offset: 100},
			}},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%T", test), func(t *testing.T) {
			b := &bytes.Buffer{}
			w := &writeBuffer{w: b}
			w.write(test)

			if size := int(sizeof(test)); size != b.Len() {
				t.Error("invalid size:", size, "!=", b.Len())
			}

			r := &readBuffer{r: b, n: b.Len()}
			v := reflect.New(reflect.TypeOf(test))

			r.readValue(v.Interface())
			if r.err != nil {
				t.Fatal(r.err)
			}
			if r.n != 0 {
				t.Errorf("%d unread bytes", r.n)
			}
			if !reflect.DeepEqual(test, v.Elem().Interface()) {
				t.Error("values don't match:")
				t.Logf("expected: %#v", test)
				t.Logf("found:    %#v", v.Elem().Interface())
			}
		})
	}
}

func testProtocolType(t *testing.T, x writable, y readable) {
	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	x.writeTo(w)

	r := &readBuffer{r: b, n: b.Len()}
	y.readFrom(r)

	if w.err != nil {
		t.Fatalf("unexpected error writing %T: %s", x, w.err)
	}

	if r.err != nil {
		t.Fatalf("unexpected error reading %T: %s", y, r.err)
	}

	if r.n != 0 {
		t.Fatalf("expected 0 remaining bytes reading %T, got %v", y, r.n)
	}

	if !reflect.DeepEqual(x, y) {
		t.Errorf("expected %T and %T values to be the same", x, y)
	}
}
