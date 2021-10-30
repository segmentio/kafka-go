package kafka

import (
	"context"
	"fmt"
)

// GetTopicPartitions retrieves partition information for the input topics,
// returning a map whose key is the topic name and value is the partition
// metadata from the Kafka brokers.
func (c *Client) GetTopicPartitions(ctx context.Context, topics ...string) (map[string][]Partition, error) {
	req := MetadataRequest{Topics: topics}
	res, err := c.Metadata(ctx, &req)
	if err != nil {
		return nil, fmt.Errorf("kafka.(*Client).GetTopicPartitions for Topics %v: %w", topics, err)
	}

	output := make(map[string][]Partition, len(res.Topics))
	for _, topic := range res.Topics {
		if topic.Error != nil {
			return nil, fmt.Errorf("kafka.(*Client).GetTopicPartitions for Topic %s: %w", topic.Name, topic.Error)
		}

		output[topic.Name] = topic.Partitions
	}
	return output, nil
}
