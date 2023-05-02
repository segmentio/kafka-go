package kafka

import (
	"bufio"
	"bytes"
	"fmt"
)

const defaultGeneration = -1

type topicPartitionAssignment struct {
	Topic     string
	Partition int32
}

type StickyAssignorUserData interface {
	partitions() []topicPartitionAssignment
	hasGeneration() bool
	generation() int
}

type StickyAssignorUserDataV2 struct {
	Topics          map[string][]int32
	Generation      int32
	topicPartitions []topicPartitionAssignment
}

func (s StickyAssignorUserDataV2) writeTo(wb *writeBuffer) {
	wb.writeInt32(int32(len(s.Topics)))

	for topic, partitions := range s.Topics {
		wb.writeString(topic)
		wb.writeInt32Array(partitions)
	}
	wb.writeInt32((s.Generation))
}
func (t StickyAssignorUserDataV2) bytes() []byte {
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

func (t topicPartitionAssignment) bytes() []byte {
	buf := bytes.NewBuffer(nil)
	t.writeTo(&writeBuffer{w: buf})
	return buf.Bytes()
}

func (t *topicPartitionAssignment) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readString(r, size, &t.Topic); err != nil {
		return
	}
	if remain, err = readInt32(r, remain, &t.Partition); err != nil {
		return
	}
	return
}

func (t *StickyAssignorUserDataV2) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readMapStringInt32(r, size, &t.Topics); err != nil {
		return
	}
	if remain, err = readInt32(r, remain, &t.Generation); err != nil {
		return
	}
	fmt.Println("mytopics", t.Topics)
	t.topicPartitions = populateTopicPartitions(t.Topics)
	return
}
func (m *StickyAssignorUserDataV2) partitions() []topicPartitionAssignment { return m.topicPartitions }
func (m *StickyAssignorUserDataV2) hasGeneration() bool                    { return false }
func (m *StickyAssignorUserDataV2) generation() int                        { return defaultGeneration }

func populateTopicPartitions(topics map[string][]int32) []topicPartitionAssignment {
	topicPartitions := make([]topicPartitionAssignment, 0)
	fmt.Println("in populatetopicpartitions")
	for topic, partitions := range topics {
		fmt.Println("in for loop of populatetopicpartitions")
		fmt.Println("topic,partitions ", topic, partitions)
		for _, partition := range partitions {
			topicPartitions = append(topicPartitions, topicPartitionAssignment{Topic: topic, Partition: partition})
		}
	}
	return topicPartitions
}
