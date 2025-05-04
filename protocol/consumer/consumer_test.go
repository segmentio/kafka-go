package consumer_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/consumer"
)

func TestSubscription(t *testing.T) {
	subscription := consumer.Subscription{
		Topics:   []string{"topic-1", "topic-2"},
		UserData: []byte("user-data"),
		OwnedPartitions: []consumer.TopicPartition{
			{
				Topic:      "topic-1",
				Partitions: []int32{1, 2, 3},
			},
		},
		GenerationID: 10,
		RackID:       "rack",
	}

	for _, version := range []int16{3, 2, 1, 0} {
		sub := subscription
		if version == 0 {
			sub.OwnedPartitions = nil
		}
		if version < 2 {
			sub.GenerationID = 0
		}
		if version < 3 {
			sub.RackID = ""
		}
		data, err := protocol.Marshal(version, sub)
		if err != nil {
			t.Fatal(err)
		}
		var gotSubscription consumer.Subscription
		err = protocol.Unmarshal(data, version, &gotSubscription)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(sub, gotSubscription) {
			t.Fatalf("unexpected result after marshal/unmarshal \nexpected\n %#v\ngot\n %#v", subscription, gotSubscription)
		}
	}
}

func TestInvalidVersion1(t *testing.T) {
	groupMemberMetadataV1MissingOwnedPartitions := []byte{
		0, 1, // Version
		0, 0, 0, 2, // Topic array length
		0, 3, 'o', 'n', 'e', // Topic one
		0, 3, 't', 'w', 'o', // Topic two
		0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
	}

	var s consumer.Subscription
	err := s.FromBytes(groupMemberMetadataV1MissingOwnedPartitions)
	if err != nil {
		t.Fatal(err)
	}

	if s.Version != 1 {
		t.Fatalf("expected version to be 1 got: %d", s.Version)
	}

	if !reflect.DeepEqual(s.Topics, []string{"one", "two"}) {
		t.Fatalf("expected topics to be [one two] got: %v", s.Topics)
	}

	if !bytes.Equal(s.UserData, []byte{0x01, 0x02, 0x03}) {
		t.Fatalf(`expected user data to be [1 2 3] got: %v`, s.UserData)
	}
}
