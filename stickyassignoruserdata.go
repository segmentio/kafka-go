package kafka

import (
	"bufio"
	"bytes"
)

const defaultGeneration int32 = -1

type topicPartitionAssignment struct {
	Topic     string
	Partition int32
}

type StickyAssignorUserData interface {
	partitions() []topicPartitionAssignment
	hasGeneration() bool
	generation() int32
}

type StickyAssignorUserDataV0 struct {
	Topics          map[string][]int32
	Generation      int32
	topicPartitions []topicPartitionAssignment
}

func (s StickyAssignorUserDataV0) writeTo(wb *writeBuffer) {
	wb.writeInt32(int32(len(s.Topics)))

	for topic, partitions := range s.Topics {
		wb.writeString(topic)
		wb.writeInt32Array(partitions)
	}
	wb.writeInt32(s.Generation)
}
func (t StickyAssignorUserDataV0) bytes() []byte {
	buf := bytes.NewBuffer(nil)
	t.writeTo(&writeBuffer{w: buf})
	return buf.Bytes()
}
func (t topicPartitionAssignment) size() int32 {
	return sizeofString(t.Topic) + 4
}

func (t topicPartitionAssignment) writeTo(wb *writeBuffer) {
	wb.writeString(t.Topic)
	wb.writeInt32((t.Partition))
}

func (t *StickyAssignorUserDataV0) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readMapStringInt32(r, size, &t.Topics); err != nil {
		return
	}
	if remain, err = readInt32(r, remain, &t.Generation); err != nil {
		return
	}
	t.topicPartitions = populateTopicPartitions(t.Topics)
	return
}
func (m *StickyAssignorUserDataV0) partitions() []topicPartitionAssignment { return m.topicPartitions }
func (m *StickyAssignorUserDataV0) hasGeneration() bool                    { return true }
func (m *StickyAssignorUserDataV0) generation() int32                      { return m.Generation }

func populateTopicPartitions(topics map[string][]int32) []topicPartitionAssignment {
	topicPartitions := make([]topicPartitionAssignment, 0)
	for topic, partitions := range topics {
		for _, partition := range partitions {
			topicPartitions = append(topicPartitions, topicPartitionAssignment{Topic: topic, Partition: partition})
		}
	}
	return topicPartitions
}
