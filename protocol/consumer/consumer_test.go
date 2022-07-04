package consumer_test

import (
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
	}

	for _, version := range []int16{1, 0} {
		if version == 0 {
			subscription.OwnedPartitions = nil
		}
		data, err := protocol.Marshal(version, subscription)
		if err != nil {
			t.Fatal(err)
		}
		var gotSubscription consumer.Subscription
		err = protocol.Unmarshal(data, version, &gotSubscription)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(subscription, gotSubscription) {
			t.Fatalf("unexpected result after marshal/unmarshal \nexpected\n %#v\ngot\n %#v", subscription, gotSubscription)
		}
	}
}
