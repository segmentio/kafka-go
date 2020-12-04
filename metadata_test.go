package kafka

import (
	"context"
	"testing"
)

func TestClientMetadata(t *testing.T) {
	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	metadata, err := client.Metadata(context.Background(), &MetadataRequest{
		Topics: []string{topic},
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(metadata.Brokers) == 0 {
		t.Error("no brokers were returned in the metadata response")
	}

	for _, b := range metadata.Brokers {
		if b == (Broker{}) {
			t.Error("unexpected broker with zero-value in metadata response")
		}
	}

	if len(metadata.Topics) == 0 {
		t.Error("no topics were returned in the metadata response")
	} else {
		topicMetadata := metadata.Topics[0]

		if topicMetadata.Name != topic {
			t.Error("invalid topic name:", topicMetadata.Name)
		}

		if len(topicMetadata.Partitions) == 0 {
			t.Error("no partitions were returned in the topic metadata response")
		} else {
			partitionMetadata := topicMetadata.Partitions[0]

			if partitionMetadata.Topic != topic {
				t.Error("invalid partition topic name:", partitionMetadata.Topic)
			}

			if partitionMetadata.ID != 0 {
				t.Error("invalid partition index:", partitionMetadata.ID)
			}

			if partitionMetadata.Leader == (Broker{}) {
				t.Error("no partition leader was returned in the partition metadata response")
			}

			if partitionMetadata.Error != nil {
				t.Error("unexpected error found in the partition metadata response:", partitionMetadata.Error)
			}

			// assume newLocalClientAndTopic creates the topic with one
			// partition
			if len(topicMetadata.Partitions) > 1 {
				t.Error("too many partitions were returned in the topic metadata response")
			}
		}

		if topicMetadata.Error != nil {
			t.Error("unexpected error found in the topic metadata response:", topicMetadata.Error)
		}

		if len(metadata.Topics) > 1 {
			t.Error("too many topics were returned in the metadata response")
		}
	}
}
