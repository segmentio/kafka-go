package kafka

import (
	"testing"
	"time"
)

func TestOffsetLog(t *testing.T) {
	testOffsetStore(t, func() (offsetStore, func()) {
		c, err := Dial("tcp", "localhost:9092")
		if err != nil {
			panic(err)
		}

		topic := makeTopic()

		if err := c.CreateTopics(TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}); err != nil {
			panic(err)
		}

		time.Sleep(100 * time.Millisecond)

		log := &offsetLog{
			brokers:   []string{"localhost:9092"},
			topic:     topic,
			partition: 0,
		}

		return log, func() { c.DeleteTopics(topic) }
	})
}
