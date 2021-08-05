package kafka

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestListGroupsResponseV1(t *testing.T) {
	item := listGroupsResponseV1{
		ErrorCode: 2,
		Groups: []listGroupsResponseGroupV1{
			{
				GroupID:      "a",
				ProtocolType: "b",
			},
		},
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found listGroupsResponseV1
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

func TestClientListGroups(t *testing.T) {
	client, shutdown := newLocalClient()
	defer shutdown()

	topic := makeTopic()
	gid := fmt.Sprintf("%s-test-group", topic)

	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	w := newTestWriter(WriterConfig{
		Topic: topic,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := w.WriteMessages(
		ctx,
		Message{
			Key:   []byte("key"),
			Value: []byte("value"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	r := NewReader(ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic,
		GroupID:  gid,
		MinBytes: 10,
		MaxBytes: 1000,
	})
	_, err = r.ReadMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.ListGroups(
		ctx,
		&ListGroupsRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error != nil {
		t.Error(
			"Unexpected error in response",
			"expected", nil,
			"got", resp.Error,
		)
	}
	hasGroup := false
	for _, group := range resp.Groups {
		if group.GroupID == gid {
			hasGroup = true
			break
		}
	}

	if !hasGroup {
		t.Error("Group not found in list")
	}
}
