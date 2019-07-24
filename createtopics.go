package kafka

import (
	"time"
)

type ConfigEntry struct {
	ConfigName  string
	ConfigValue string
}

func (c ConfigEntry) toCreateTopicsRequestV0ConfigEntry() createTopicsRequestV0ConfigEntry {
	return createTopicsRequestV0ConfigEntry{
		ConfigName:  c.ConfigName,
		ConfigValue: c.ConfigValue,
	}
}

type createTopicsRequestV0ConfigEntry struct {
	ConfigName  string
	ConfigValue string
}

func (t createTopicsRequestV0ConfigEntry) size() int32 {
	return sizeofString(t.ConfigName) +
		sizeofString(t.ConfigValue)
}

func (t createTopicsRequestV0ConfigEntry) writeTo(wb *writeBuffer) {
	wb.writeString(t.ConfigName)
	wb.writeString(t.ConfigValue)
}

type ReplicaAssignment struct {
	Partition int
	Replicas  int
}

func (a ReplicaAssignment) toCreateTopicsRequestV0ReplicaAssignment() createTopicsRequestV0ReplicaAssignment {
	return createTopicsRequestV0ReplicaAssignment{
		Partition: int32(a.Partition),
		Replicas:  int32(a.Replicas),
	}
}

type createTopicsRequestV0ReplicaAssignment struct {
	Partition int32
	Replicas  int32
}

func (t createTopicsRequestV0ReplicaAssignment) size() int32 {
	return sizeofInt32(t.Partition) +
		sizeofInt32(t.Replicas)
}

func (t createTopicsRequestV0ReplicaAssignment) writeTo(wb *writeBuffer) {
	wb.writeInt32(t.Partition)
	wb.writeInt32(t.Replicas)
}

type TopicConfig struct {
	// Topic name
	Topic string

	// NumPartitions created. -1 indicates unset.
	NumPartitions int

	// ReplicationFactor for the topic. -1 indicates unset.
	ReplicationFactor int

	// ReplicaAssignments among kafka brokers for this topic partitions. If this
	// is set num_partitions and replication_factor must be unset.
	ReplicaAssignments []ReplicaAssignment

	// ConfigEntries holds topic level configuration for topic to be set.
	ConfigEntries []ConfigEntry
}

func (t TopicConfig) toCreateTopicsRequestV0Topic() createTopicsRequestV0Topic {
	var requestV0ReplicaAssignments []createTopicsRequestV0ReplicaAssignment
	for _, a := range t.ReplicaAssignments {
		requestV0ReplicaAssignments = append(
			requestV0ReplicaAssignments,
			a.toCreateTopicsRequestV0ReplicaAssignment())
	}
	var requestV0ConfigEntries []createTopicsRequestV0ConfigEntry
	for _, c := range t.ConfigEntries {
		requestV0ConfigEntries = append(
			requestV0ConfigEntries,
			c.toCreateTopicsRequestV0ConfigEntry())
	}

	return createTopicsRequestV0Topic{
		Topic:              t.Topic,
		NumPartitions:      int32(t.NumPartitions),
		ReplicationFactor:  int16(t.ReplicationFactor),
		ReplicaAssignments: requestV0ReplicaAssignments,
		ConfigEntries:      requestV0ConfigEntries,
	}
}

type createTopicsRequestV0Topic struct {
	// Topic name
	Topic string

	// NumPartitions created. -1 indicates unset.
	NumPartitions int32

	// ReplicationFactor for the topic. -1 indicates unset.
	ReplicationFactor int16

	// ReplicaAssignments among kafka brokers for this topic partitions. If this
	// is set num_partitions and replication_factor must be unset.
	ReplicaAssignments []createTopicsRequestV0ReplicaAssignment

	// ConfigEntries holds topic level configuration for topic to be set.
	ConfigEntries []createTopicsRequestV0ConfigEntry
}

func (t createTopicsRequestV0Topic) size() int32 {
	return sizeofString(t.Topic) +
		sizeofInt32(t.NumPartitions) +
		sizeofInt16(t.ReplicationFactor) +
		sizeofArray(len(t.ReplicaAssignments), func(i int) int32 { return t.ReplicaAssignments[i].size() }) +
		sizeofArray(len(t.ConfigEntries), func(i int) int32 { return t.ConfigEntries[i].size() })
}

func (t createTopicsRequestV0Topic) writeTo(wb *writeBuffer) {
	wb.writeString(t.Topic)
	wb.writeInt32(t.NumPartitions)
	wb.writeInt16(t.ReplicationFactor)
	wb.writeArray(len(t.ReplicaAssignments), func(i int) { t.ReplicaAssignments[i].writeTo(wb) })
	wb.writeArray(len(t.ConfigEntries), func(i int) { t.ConfigEntries[i].writeTo(wb) })
}

// See http://kafka.apache.org/protocol.html#The_Messages_CreateTopics
type createTopicsRequestV0 struct {
	// Topics contains n array of single topic creation requests. Can not
	// have multiple entries for the same topic.
	Topics []createTopicsRequestV0Topic

	// Timeout ms to wait for a topic to be completely created on the
	// controller node. Values <= 0 will trigger topic creation and return immediately
	Timeout int32
}

func (t createTopicsRequestV0) size() int32 {
	return sizeofArray(len(t.Topics), func(i int) int32 { return t.Topics[i].size() }) +
		sizeofInt32(t.Timeout)
}

func (t createTopicsRequestV0) writeTo(wb *writeBuffer) {
	wb.writeArray(len(t.Topics), func(i int) { t.Topics[i].writeTo(wb) })
	wb.writeInt32(t.Timeout)
}

type createTopicsResponseV0TopicError struct {
	// Topic name
	Topic string

	// ErrorCode holds response error code
	ErrorCode int16
}

func (t createTopicsResponseV0TopicError) size() int32 {
	return sizeofString(t.Topic) +
		sizeofInt16(t.ErrorCode)
}

func (t createTopicsResponseV0TopicError) writeTo(wb *writeBuffer) {
	wb.writeString(t.Topic)
	wb.writeInt16(t.ErrorCode)
}

func (t *createTopicsResponseV0TopicError) readFrom(rb *readBuffer) {
	t.Topic = rb.readString()
	t.ErrorCode = rb.readInt16()
}

// See http://kafka.apache.org/protocol.html#The_Messages_CreateTopics
type createTopicsResponseV0 struct {
	TopicErrors []createTopicsResponseV0TopicError
}

func (t createTopicsResponseV0) size() int32 {
	return sizeofArray(len(t.TopicErrors), func(i int) int32 { return t.TopicErrors[i].size() })
}

func (t createTopicsResponseV0) writeTo(wb *writeBuffer) {
	wb.writeArray(len(t.TopicErrors), func(i int) { t.TopicErrors[i].writeTo(wb) })
}

func (t *createTopicsResponseV0) readFrom(rb *readBuffer) {
	rb.readArray(func() {
		topicError := createTopicsResponseV0TopicError{}
		topicError.readFrom(rb)
		t.TopicErrors = append(t.TopicErrors, topicError)
	})
}

func (c *Conn) createTopics(req createTopicsRequestV0) (createTopicsResponseV0, error) {
	var res createTopicsResponseV0

	err := c.writeOperation(
		func(deadline time.Time, id int32) {
			if req.Timeout == 0 {
				now := time.Now()
				deadline = adjustDeadlineForRTT(deadline, now, defaultRTT)
				req.Timeout = milliseconds(deadlineToTimeout(deadline, now))
			}
			c.writeRequest(createTopicsRequest, v0, id, req)
		},
		func(deadline time.Time) {
			res.readFrom(&c.rb)
		},
	)

	if err != nil {
		return res, err
	}

	for _, tr := range res.TopicErrors {
		if tr.ErrorCode != 0 {
			return res, Error(tr.ErrorCode)
		}
	}

	return res, nil
}

// CreateTopics creates one topic per provided configuration with idempotent
// operational semantics. In other words, if CreateTopics is invoked with a
// configuration for an existing topic, it will have no effect.
func (c *Conn) CreateTopics(topics ...TopicConfig) error {
	var requestV0Topics []createTopicsRequestV0Topic

	for _, t := range topics {
		requestV0Topics = append(requestV0Topics, t.toCreateTopicsRequestV0Topic())
	}

	_, err := c.createTopics(createTopicsRequestV0{
		Topics: requestV0Topics,
	})

	switch err {
	case TopicAlreadyExists:
		return nil
	default:
		return err
	}
}
