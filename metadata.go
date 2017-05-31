package kafka

import "bufio"

type topicMetadataRequestV0 []string

func (r topicMetadataRequestV0) size() int32 {
	return sizeofStringArray([]string(r))
}

func (r topicMetadataRequestV0) writeTo(w *bufio.Writer) {
	writeStringArray(w, []string(r))
}

type metadataResponseV0 struct {
	Brokers []brokerMetadataV0
	Topics  []topicMetadataV0
}

func (r metadataResponseV0) size() int32 {
	n1 := sizeofArray(len(r.Brokers), func(i int) int32 { return r.Brokers[i].size() })
	n2 := sizeofArray(len(r.Topics), func(i int) int32 { return r.Topics[i].size() })
	return n1 + n2
}

func (r metadataResponseV0) writeTo(w *bufio.Writer) {
	writeArray(w, len(r.Brokers), func(i int) { r.Brokers[i].writeTo(w) })
	writeArray(w, len(r.Topics), func(i int) { r.Topics[i].writeTo(w) })
}

type brokerMetadataV0 struct {
	NodeID int32
	Host   string
	Port   int32
}

func (b brokerMetadataV0) size() int32 {
	return 4 + 4 + sizeofString(b.Host)
}

func (b brokerMetadataV0) writeTo(w *bufio.Writer) {
	writeInt32(w, b.NodeID)
	writeString(w, b.Host)
	writeInt32(w, b.Port)
}

type topicMetadataV0 struct {
	TopicErrorCode int16
	TopicName      string
	Partitions     []partitionMetadataV0
}

func (t topicMetadataV0) size() int32 {
	return 2 +
		sizeofString(t.TopicName) +
		sizeofArray(len(t.Partitions), func(i int) int32 { return t.Partitions[i].size() })
}

func (t topicMetadataV0) writeTo(w *bufio.Writer) {
	writeInt16(w, t.TopicErrorCode)
	writeString(w, t.TopicName)
	writeArray(w, len(t.Partitions), func(i int) { t.Partitions[i].writeTo(w) })
}

type partitionMetadataV0 struct {
	PartitionErrorCode int16
	PartitionID        int32
	Leader             int32
	Replicas           []int32
	Isr                []int32
}

func (p partitionMetadataV0) size() int32 {
	return 2 + 4 + 4 + sizeofInt32Array(p.Replicas) + sizeofInt32Array(p.Isr)
}

func (p partitionMetadataV0) writeTo(w *bufio.Writer) {
	writeInt16(w, p.PartitionErrorCode)
	writeInt32(w, p.PartitionID)
	writeInt32(w, p.Leader)
	writeInt32Array(w, p.Replicas)
	writeInt32Array(w, p.Isr)
}
