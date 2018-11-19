package kafka

import "testing"

func CreateTopic(t *testing.T, partitions int) string {
	topic := makeTopic()
	createTopic(t, topic, partitions)
	return topic
}

func SetMaxMessageBytes(conn *Conn, maxMsgBytes int32) {
	conn.maxMsgBytes = maxMsgBytes
}
