package kafka

import (
	"context"
	"fmt"
)

// TopicPartitionCount returns the number of partitions for a given topic, which
// can be used by the caller for any sort of balancing/partitioning strategy.
func (c *Client) TopicPartitionCount(ctx context.Context, topic string) (int, error) {
	req := MetadataRequest{Topics: []string{topic}}
	res, err := c.Metadata(ctx, &req)
	if err != nil {
		return 0, fmt.Errorf("kafka.(*Client).TopicPartitionCount for Topic '%s': %w", topic, err)
	}

	if len(res.Topics) > 0 {
		t := res.Topics[0]

		if t.Error != nil {
			return 0, fmt.Errorf("kafka.(*Client).TopicPartitionCount for Topic '%s': %w", topic, t.Error)
		}

		return len(t.Partitions), nil
	}

	return 0, UnknownTopicOrPartition
}
