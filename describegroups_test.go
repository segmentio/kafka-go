package kafka

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestClientDescribeGroups(t *testing.T) {
	if os.Getenv("KAFKA_VERSION") == "2.3.1" {
		// There's a bug in 2.3.1 that causes the MemberMetadata to be in the wrong format and thus
		// leads to an error when decoding the DescribeGroupsResponse.
		//
		// See https://issues.apache.org/jira/browse/KAFKA-9150 for details.
		t.Skip("Skipping because kafka version is 2.3.1")
	}

	client, shutdown := newLocalClient()
	defer shutdown()

	topic := makeTopic()
	gid := fmt.Sprintf("%s-test-group", topic)

	createTopic(t, topic, 2)
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

	resp, err := client.DescribeGroups(
		ctx,
		&DescribeGroupsRequest{
			GroupIDs: []string{gid},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Groups) != 1 {
		t.Fatal(
			"Unexpected number of groups returned",
			"expected", 1,
			"got", len(resp.Groups),
		)
	}
	g := resp.Groups[0]
	if g.Error != nil {
		t.Error(
			"Wrong error in group response",
			"expected", nil,
			"got", g.Error,
		)
	}

	if g.GroupID != gid {
		t.Error(
			"Wrong groupID",
			"expected", gid,
			"got", g.GroupID,
		)
	}

	if len(g.Members) != 1 {
		t.Fatal(
			"Wrong group members length",
			"expected", 1,
			"got", len(g.Members),
		)
	}
	if len(g.Members[0].MemberAssignments.Topics) != 1 {
		t.Fatal(
			"Wrong topics length",
			"expected", 1,
			"got", len(g.Members[0].MemberAssignments.Topics),
		)
	}
	mt := g.Members[0].MemberAssignments.Topics[0]
	if mt.Topic != topic {
		t.Error(
			"Wrong member assignment topic",
			"expected", topic,
			"got", mt.Topic,
		)
	}

	// Partitions can be in any order, sort them
	sort.Slice(mt.Partitions, func(a, b int) bool {
		return mt.Partitions[a] < mt.Partitions[b]
	})

	if !reflect.DeepEqual([]int{0, 1}, mt.Partitions) {
		t.Error(
			"Wrong member assignment partitions",
			"expected", []int{0, 1},
			"got", mt.Partitions,
		)
	}
}
