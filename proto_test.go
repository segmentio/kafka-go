package kafka

import (
	"bufio"
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

func TestProtocol(t *testing.T) {
	tests := []interface{}{
		requestHeader{
			Size:          26,
			ApiKey:        int16(offsetCommitRequest),
			ApiVersion:    int16(v2),
			CorrelationID: 42,
			ClientID:      "Hello World!",
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
