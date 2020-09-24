package kafka

type topicMetadataRequestV1 []string

func (r topicMetadataRequestV1) size() int32 {
	return sizeofStringArray([]string(r))
}

func (r topicMetadataRequestV1) writeTo(wb *writeBuffer) {
	// communicate nil-ness to the broker by passing -1 as the array length.
	// for this particular request, the broker interpets a zero length array
	// as a request for no topics whereas a nil array is for all topics.
	if r == nil {
		wb.writeArrayLen(-1)
	} else {
		wb.writeStringArray([]string(r))
	}
}

type metadataResponseV1 struct {
	Brokers      []brokerMetadataV1
	ControllerID int32
	Topics       []topicMetadataV1
}

func (r metadataResponseV1) size() int32 {
	n1 := sizeofArray(len(r.Brokers), func(i int) int32 { return r.Brokers[i].size() })
	n2 := sizeofArray(len(r.Topics), func(i int) int32 { return r.Topics[i].size() })
	return 4 + n1 + n2
}

func (r metadataResponseV1) writeTo(wb *writeBuffer) {
	wb.writeArray(len(r.Brokers), func(i int) { r.Brokers[i].writeTo(wb) })
	wb.writeInt32(r.ControllerID)
	wb.writeArray(len(r.Topics), func(i int) { r.Topics[i].writeTo(wb) })
}

type brokerMetadataV1 struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   string
}

func (b brokerMetadataV1) size() int32 {
	return 4 + 4 + sizeofString(b.Host) + sizeofString(b.Rack)
}

func (b brokerMetadataV1) writeTo(wb *writeBuffer) {
	wb.writeInt32(b.NodeID)
	wb.writeString(b.Host)
	wb.writeInt32(b.Port)
	wb.writeString(b.Rack)
}

type topicMetadataV1 struct {
	TopicErrorCode int16
	TopicName      string
	Internal       bool
	Partitions     []partitionMetadataV1
}

func (t topicMetadataV1) size() int32 {
	return 2 + 1 +
		sizeofString(t.TopicName) +
		sizeofArray(len(t.Partitions), func(i int) int32 { return t.Partitions[i].size() })
}

func (t topicMetadataV1) writeTo(wb *writeBuffer) {
	wb.writeInt16(t.TopicErrorCode)
	wb.writeString(t.TopicName)
	wb.writeBool(t.Internal)
	wb.writeArray(len(t.Partitions), func(i int) { t.Partitions[i].writeTo(wb) })
}

type partitionMetadataV1 struct {
	PartitionErrorCode int16
	PartitionID        int32
	Leader             int32
	Replicas           []int32
	Isr                []int32
}

func (p partitionMetadataV1) size() int32 {
	return 2 + 4 + 4 + sizeofInt32Array(p.Replicas) + sizeofInt32Array(p.Isr)
}

func (p partitionMetadataV1) writeTo(wb *writeBuffer) {
	wb.writeInt16(p.PartitionErrorCode)
	wb.writeInt32(p.PartitionID)
	wb.writeInt32(p.Leader)
	wb.writeInt32Array(p.Replicas)
	wb.writeInt32Array(p.Isr)
}

type topicMetadataRequestV5 struct {
	Topics                 []string
	AllowAutoTopicCreation bool
}

func (t topicMetadataRequestV5) size() int32 {
	return 1 + sizeofStringArray(t.Topics)
}

func (t topicMetadataRequestV5) writeTo(wb *writeBuffer) {
	// communicate nil-ness to the broker by passing -1 as the array length.
	// for this particular request, the broker interpets a zero length array
	// as a request for no topics whereas a nil array is for all topics.
	if t.Topics == nil {
		wb.writeArrayLen(-1)
	} else {
		wb.writeStringArray(t.Topics)
	}

	wb.writeBool(t.AllowAutoTopicCreation)
}

type metadataResponseV5 struct {
	ThrottleTime int32
	Brokers      []brokerMetadataV5
	ClusterID    string
	ControllerID int32
	Topics       []topicMetadataV5
}

func (r metadataResponseV5) size() int32 {
	n1 := sizeofArray(len(r.Brokers), func(i int) int32 { return r.Brokers[i].size() })
	n2 := sizeofArray(len(r.Topics), func(i int) int32 { return r.Topics[i].size() })
	return 4 + 4 + n1 + 4 + 4 + n2
}

func (r metadataResponseV5) writeTo(wb *writeBuffer) {
	wb.writeInt32(r.ThrottleTime)
	wb.writeArray(len(r.Brokers), func(i int) { r.Brokers[i].writeTo(wb) })
	wb.WriteString(r.ClusterID)
	wb.writeInt32(r.ControllerID)
	wb.writeArray(len(r.Topics), func(i int) { r.Topics[i].writeTo(wb) })
}

type brokerMetadataV5 struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   string
}

func (b brokerMetadataV5) size() int32 {
	return 4 + 4 + sizeofString(b.Host) + sizeofString(b.Rack)
}

func (b brokerMetadataV5) writeTo(wb *writeBuffer) {
	wb.writeInt32(b.NodeID)
	wb.writeString(b.Host)
	wb.writeInt32(b.Port)
	wb.writeString(b.Rack)
}

type topicMetadataV5 struct {
	TopicErrorCode int16
	TopicName      string
	Internal       bool
	Partitions     []partitionMetadataV5
}

func (t topicMetadataV5) size() int32 {
	return 2 + 1 +
		sizeofString(t.TopicName) +
		sizeofArray(len(t.Partitions), func(i int) int32 { return t.Partitions[i].size() })
}

func (t topicMetadataV5) writeTo(wb *writeBuffer) {
	wb.writeInt16(t.TopicErrorCode)
	wb.writeString(t.TopicName)
	wb.writeBool(t.Internal)
	wb.writeArray(len(t.Partitions), func(i int) { t.Partitions[i].writeTo(wb) })
}

type partitionMetadataV5 struct {
	PartitionErrorCode int16
	PartitionID        int32
	Leader             int32
	Replicas           []int32
	Isr                []int32
	OfflineReplicas    []int32
}

func (p partitionMetadataV5) size() int32 {
	return 2 + 4 + 4 + sizeofInt32Array(p.Replicas) + sizeofInt32Array(p.Isr) + sizeofInt32Array(p.OfflineReplicas)
}

func (p partitionMetadataV5) writeTo(wb *writeBuffer) {
	wb.writeInt16(p.PartitionErrorCode)
	wb.writeInt32(p.PartitionID)
	wb.writeInt32(p.Leader)
	wb.writeInt32Array(p.Replicas)
	wb.writeInt32Array(p.Isr)
	wb.writeInt32Array(p.OfflineReplicas)
}
