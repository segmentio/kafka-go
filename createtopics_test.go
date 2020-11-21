package kafka

import (
	"bufio"
	"bytes"
	"context"
	"reflect"
	"testing"
)

func TestClientCreateTopics(t *testing.T) {
	const (
		topic1 = "client-topic-1"
		topic2 = "client-topic-2"
		topic3 = "client-topic-3"
	)

	client, shutdown := newLocalClient()
	defer shutdown()

	config := []ConfigEntry{{
		ConfigName:  "retention.ms",
		ConfigValue: "3600000",
	}}

	res, err := client.CreateTopics(context.Background(), &CreateTopicsRequest{
		Topics: []TopicConfig{
			{
				Topic:             topic1,
				NumPartitions:     -1,
				ReplicationFactor: -1,
				ReplicaAssignments: []ReplicaAssignment{
					{
						Partition: 0,
						Replicas:  []int{1},
					},
					{
						Partition: 1,
						Replicas:  []int{1},
					},
					{
						Partition: 2,
						Replicas:  []int{1},
					},
				},
				ConfigEntries: config,
			},
			{
				Topic:             topic2,
				NumPartitions:     2,
				ReplicationFactor: 1,
				ConfigEntries:     config,
			},
			{
				Topic:             topic3,
				NumPartitions:     1,
				ReplicationFactor: 1,
				ConfigEntries:     config,
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	defer deleteTopic(t, topic1, topic2, topic3)

	expectTopics := map[string]struct{}{
		topic1: {},
		topic2: {},
		topic3: {},
	}

	for topic, error := range res.Errors {
		delete(expectTopics, topic)

		if error != nil {
			t.Errorf("%s => %s", topic, error)
		}
	}

	for topic := range expectTopics {
		t.Errorf("topic missing in response: %s", topic)
	}
}

func TestCreateTopicsResponseV0(t *testing.T) {
	item := createTopicsResponseV0{
		TopicErrors: []createTopicsResponseV0TopicError{
			{
				Topic:     "topic",
				ErrorCode: 2,
			},
		},
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found createTopicsResponseV0
	remain, err := (&found).readFrom(bufio.NewReader(b), b.Len())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if remain != 0 {
		t.Errorf("expected 0 remain, got %v", remain)
		t.FailNow()
	}
	if !reflect.DeepEqual(item, found) {
		t.Error("expected item and found to be the same")
		t.FailNow()
	}
}
