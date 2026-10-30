package kafka

import (
	"context"
	"net"
	"testing"

	metadataAPI "github.com/segmentio/kafka-go/protocol/metadata"
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

func TestClientMetadataPreservesMissingBrokerIDsAndOfflineReplicas(t *testing.T) {
	// Synthetic Metadata: broker 1 is live; broker 2 is down (absent from the
	// broker list but still named by the partitions). Partition 1 is leaderless.
	client := &Client{
		Addr:      TCP("unused.invalid:9092"),
		Transport: syntheticMetadataTransport{},
	}
	response, err := client.Metadata(context.Background(), &MetadataRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Topics) != 1 || len(response.Topics[0].Partitions) != 2 {
		t.Fatalf("unexpected topics/partitions: %+v", response.Topics)
	}

	p0 := response.Topics[0].Partitions[0]
	assertBrokerIDs(t, "partition 0 replicas", brokerIDs(p0.Replicas), []int{1, 2})
	assertBrokerIDs(t, "partition 0 offline replicas", brokerIDs(p0.OfflineReplicas), []int{2})
	if p0.Leader.ID != 1 {
		t.Errorf("partition 0 leader ID = %d, want 1", p0.Leader.ID)
	}

	p1 := response.Topics[0].Partitions[1]
	if p1.Leader.ID != -1 {
		t.Errorf("partition 1 leader ID = %d, want -1", p1.Leader.ID)
	}
	assertBrokerIDs(t, "partition 1 replicas", brokerIDs(p1.Replicas), []int{2})
	assertBrokerIDs(t, "partition 1 isr", brokerIDs(p1.Isr), []int{2})
	assertBrokerIDs(t, "partition 1 offline replicas", brokerIDs(p1.OfflineReplicas), []int{2})
}

type syntheticMetadataTransport struct{}

func (syntheticMetadataTransport) RoundTrip(context.Context, net.Addr, Request) (Response, error) {
	return &metadataAPI.Response{
		ClusterID:    "synthetic-cluster",
		ControllerID: 1,
		Brokers: []metadataAPI.ResponseBroker{
			{NodeID: 1, Host: "broker1.invalid", Port: 9092},
		},
		Topics: []metadataAPI.ResponseTopic{{
			Name: "synthetic-topic",
			Partitions: []metadataAPI.ResponsePartition{
				{
					PartitionIndex:  0,
					LeaderID:        1,
					ReplicaNodes:    []int32{1, 2},
					IsrNodes:        []int32{1},
					OfflineReplicas: []int32{2},
				},
				{
					PartitionIndex:  1,
					LeaderID:        -1,
					ReplicaNodes:    []int32{2},
					IsrNodes:        []int32{2},
					OfflineReplicas: []int32{2},
				},
			},
		}},
	}, nil
}

func brokerIDs(brokers []Broker) []int {
	ids := make([]int, len(brokers))
	for i, broker := range brokers {
		ids[i] = broker.ID
	}
	return ids
}

func assertBrokerIDs(t *testing.T, name string, got, want []int) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: got %v, want %v", name, got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s: got %v, want %v", name, got, want)
			return
		}
	}
}
