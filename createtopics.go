package kafka

import (
	"bufio"
	"time"
)

type ConfigEntry struct {
	ConfigName  string
	ConfigValue string
}

func (c ConfigEntry) toCreateTopicsRequestConfigEntry() createTopicsRequestConfigEntry {
	return createTopicsRequestConfigEntry{
		ConfigName:  c.ConfigName,
		ConfigValue: c.ConfigValue,
	}
}

type createTopicsRequestConfigEntry struct {
	ConfigName  string
	ConfigValue string
}

func (t createTopicsRequestConfigEntry) size() int32 {
	return sizeofString(t.ConfigName) +
		sizeofString(t.ConfigValue)
}

func (t createTopicsRequestConfigEntry) writeTo(w *bufio.Writer) {
	writeString(w, t.ConfigName)
	writeString(w, t.ConfigValue)
}

type ReplicaAssignment struct {
	Partition int
	Replicas  int
}

func (a ReplicaAssignment) toCreateTopicsRequestReplicaAssignment() createTopicsRequestReplicaAssignment {
	return createTopicsRequestReplicaAssignment{
		Partition: int32(a.Partition),
		Replicas:  int32(a.Replicas),
	}
}

type createTopicsRequestReplicaAssignment struct {
	Partition int32
	Replicas  int32
}

func (t createTopicsRequestReplicaAssignment) size() int32 {
	return sizeofInt32(t.Partition) +
		sizeofInt32(t.Replicas)
}

func (t createTopicsRequestReplicaAssignment) writeTo(w *bufio.Writer) {
	writeInt32(w, t.Partition)
	writeInt32(w, t.Replicas)
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
	var requestReplicaAssignments []createTopicsRequestReplicaAssignment
	for _, a := range t.ReplicaAssignments {
		requestReplicaAssignments = append(
			requestReplicaAssignments,
			a.toCreateTopicsRequestReplicaAssignment())
	}
	var requestConfigEntries []createTopicsRequestConfigEntry
	for _, c := range t.ConfigEntries {
		requestConfigEntries = append(
			requestConfigEntries,
			c.toCreateTopicsRequestConfigEntry())
	}

	return createTopicsRequestV0Topic{
		Topic:              t.Topic,
		NumPartitions:      int32(t.NumPartitions),
		ReplicationFactor:  int16(t.ReplicationFactor),
		ReplicaAssignments: requestReplicaAssignments,
		ConfigEntries:      requestConfigEntries,
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
	ReplicaAssignments []createTopicsRequestReplicaAssignment

	// ConfigEntries holds topic level configuration for topic to be set.
	ConfigEntries []createTopicsRequestConfigEntry
}

func (t createTopicsRequestV0Topic) size() int32 {
	return sizeofString(t.Topic) +
		sizeofInt32(t.NumPartitions) +
		sizeofInt16(t.ReplicationFactor) +
		sizeofArray(len(t.ReplicaAssignments), func(i int) int32 { return t.ReplicaAssignments[i].size() }) +
		sizeofArray(len(t.ConfigEntries), func(i int) int32 { return t.ConfigEntries[i].size() })
}

func (t createTopicsRequestV0Topic) writeTo(w *bufio.Writer) {
	writeString(w, t.Topic)
	writeInt32(w, t.NumPartitions)
	writeInt16(w, t.ReplicationFactor)
	writeArray(w, len(t.ReplicaAssignments), func(i int) { t.ReplicaAssignments[i].writeTo(w) })
	writeArray(w, len(t.ConfigEntries), func(i int) { t.ConfigEntries[i].writeTo(w) })
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

func (t createTopicsRequestV0) writeTo(w *bufio.Writer) {
	writeArray(w, len(t.Topics), func(i int) { t.Topics[i].writeTo(w) })
	writeInt32(w, t.Timeout)
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

func (t createTopicsResponseV0TopicError) writeTo(w *bufio.Writer) {
	writeString(w, t.Topic)
	writeInt16(w, t.ErrorCode)
}

func (t *createTopicsResponseV0TopicError) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readString(r, size, &t.Topic); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &t.ErrorCode); err != nil {
		return
	}
	return
}

// See http://kafka.apache.org/protocol.html#The_Messages_CreateTopics
type createTopicsResponseV0 struct {
	TopicErrors []createTopicsResponseV0TopicError
}

func (t createTopicsResponseV0) size() int32 {
	return sizeofArray(len(t.TopicErrors), func(i int) int32 { return t.TopicErrors[i].size() })
}

func (t createTopicsResponseV0) writeTo(w *bufio.Writer) {
	writeArray(w, len(t.TopicErrors), func(i int) { t.TopicErrors[i].writeTo(w) })
}

func (t *createTopicsResponseV0) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	fn := func(r *bufio.Reader, size int) (fnRemain int, fnErr error) {
		var topic createTopicsResponseV0TopicError
		if fnRemain, fnErr = (&topic).readFrom(r, size); fnErr != nil {
			return
		}
		t.TopicErrors = append(t.TopicErrors, topic)
		return
	}

	if remain, err = readArrayWith(r, size, fn); err != nil {
		return
	}

	return
}

func (t TopicConfig) toCreateTopicsRequestV2Topic() createTopicsRequestV2Topic {
	var requestReplicaAssignments []createTopicsRequestReplicaAssignment
	for _, a := range t.ReplicaAssignments {
		requestReplicaAssignments = append(
			requestReplicaAssignments,
			a.toCreateTopicsRequestReplicaAssignment())
	}
	var requestConfigEntries []createTopicsRequestConfigEntry
	for _, c := range t.ConfigEntries {
		requestConfigEntries = append(
			requestConfigEntries,
			c.toCreateTopicsRequestConfigEntry())
	}

	return createTopicsRequestV2Topic{
		Topic:              t.Topic,
		NumPartitions:      int32(t.NumPartitions),
		ReplicationFactor:  int16(t.ReplicationFactor),
		ReplicaAssignments: requestReplicaAssignments,
		ConfigEntries:      requestConfigEntries,
	}
}

type createTopicsRequestV2Topic struct {
	// Topic name
	Topic string

	// NumPartitions created. -1 indicates unset.
	NumPartitions int32

	// ReplicationFactor for the topic. -1 indicates unset.
	ReplicationFactor int16

	// ReplicaAssignments among kafka brokers for this topic partitions. If this
	// is set num_partitions and replication_factor must be unset.
	ReplicaAssignments []createTopicsRequestReplicaAssignment

	// ConfigEntries holds topic level configuration for topic to be set.
	ConfigEntries []createTopicsRequestConfigEntry
}

func (t createTopicsRequestV2Topic) size() int32 {
	return sizeofString(t.Topic) +
		sizeofInt32(t.NumPartitions) +
		sizeofInt16(t.ReplicationFactor) +
		sizeofArray(len(t.ReplicaAssignments), func(i int) int32 { return t.ReplicaAssignments[i].size() }) +
		sizeofArray(len(t.ConfigEntries), func(i int) int32 { return t.ConfigEntries[i].size() })
}

func (t createTopicsRequestV2Topic) writeTo(w *bufio.Writer) {
	writeString(w, t.Topic)
	writeInt32(w, t.NumPartitions)
	writeInt16(w, t.ReplicationFactor)
	writeArray(w, len(t.ReplicaAssignments), func(i int) { t.ReplicaAssignments[i].writeTo(w) })
	writeArray(w, len(t.ConfigEntries), func(i int) { t.ConfigEntries[i].writeTo(w) })
}

// See http://kafka.apache.org/protocol.html#The_Messages_CreateTopics
type createTopicsRequestV2 struct {
	// Topics contains n array of single topic creation requests. Can not
	// have multiple entries for the same topic.
	Topics []createTopicsRequestV2Topic

	// Timeout ms to wait for a topic to be completely created on the
	// controller node. Values <= 0 will trigger topic creation and return immediately
	Timeout int32

	// ValidateOnly if true, the request will be validated, but the topic won
	// 't be created.
	ValidateOnly bool
}

func (t createTopicsRequestV2) size() int32 {
	return sizeofArray(len(t.Topics), func(i int) int32 { return t.Topics[i].size() }) +
		sizeofInt32(t.Timeout) +
		sizeofBool(t.ValidateOnly)
}

func (t createTopicsRequestV2) writeTo(w *bufio.Writer) {
	writeArray(w, len(t.Topics), func(i int) { t.Topics[i].writeTo(w) })
	writeInt32(w, t.Timeout)
	writeBool(w, t.ValidateOnly)
}

type createTopicsResponseV2TopicError struct {
	// Topic name
	Topic string

	// ErrorCode holds response error code
	ErrorCode int16

	// ErrorMessage holds the response error message
	ErrorMessage string
}

func (t createTopicsResponseV2TopicError) size() int32 {
	return sizeofString(t.Topic) +
		sizeofInt16(t.ErrorCode) +
		sizeofString(t.ErrorMessage)
}

func (t createTopicsResponseV2TopicError) writeTo(w *bufio.Writer) {
	writeString(w, t.Topic)
	writeInt16(w, t.ErrorCode)
	writeString(w, t.ErrorMessage)
}

func (t *createTopicsResponseV2TopicError) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readString(r, size, &t.Topic); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &t.ErrorCode); err != nil {
		return
	}
	if remain, err = readString(r, remain, &t.ErrorMessage); err != nil {
		return
	}
	return
}

// See http://kafka.apache.org/protocol.html#The_Messages_CreateTopics
type createTopicsResponseV2 struct {
	ThrottleTimeMS int32
	TopicErrors    []createTopicsResponseV2TopicError
}

func (t createTopicsResponseV2) size() int32 {
	return sizeofInt32(t.ThrottleTimeMS) +
		sizeofArray(len(t.TopicErrors), func(i int) int32 { return t.TopicErrors[i].size() })
}

func (t createTopicsResponseV2) writeTo(w *bufio.Writer) {
	writeInt32(w, t.ThrottleTimeMS)
	writeArray(w, len(t.TopicErrors), func(i int) { t.TopicErrors[i].writeTo(w) })
}

func (t *createTopicsResponseV2) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt32(r, size, &t.ThrottleTimeMS); err != nil {
		return
	}

	fn := func(r *bufio.Reader, size int) (fnRemain int, fnErr error) {
		var topic createTopicsResponseV2TopicError
		if fnRemain, fnErr = (&topic).readFrom(r, size); fnErr != nil {
			return
		}
		t.TopicErrors = append(t.TopicErrors, topic)
		return
	}
	if remain, err = readArrayWith(r, remain, fn); err != nil {
		return
	}

	return
}

// CreateTopics creates one topic per provided configuration with idempotent
// operational semantics. In other words, if CreateTopics is invoked with a
// configuration for an existing topic, it will have no effect.
func (c *Conn) CreateTopics(topics ...TopicConfig) error {
	createTopicsVersion, err := c.apiVersion(createTopicsRequest)
	if err != nil {
		return err
	}
	return c.createTopics(createTopicsVersion, topics...)
}

func (c *Conn) createTopics(version apiVersion, topics ...TopicConfig) error {
	var err error

	switch {
	case version >= v2:
		var requestV2Topics []createTopicsRequestV2Topic
		for _, t := range topics {
			requestV2Topics = append(
				requestV2Topics,
				t.toCreateTopicsRequestV2Topic())
		}

		_, err = c.createTopicsV2(createTopicsRequestV2{
			Topics: requestV2Topics,
		})

		switch err {
		case TopicAlreadyExists:
			// ok
			return nil
		default:
			return err
		}
	default:
		var requestV0Topics []createTopicsRequestV0Topic
		for _, t := range topics {
			requestV0Topics = append(
				requestV0Topics,
				t.toCreateTopicsRequestV0Topic())
		}

		_, err = c.createTopicsV0(createTopicsRequestV0{
			Topics: requestV0Topics,
		})

	}

	switch err {
	case TopicAlreadyExists:
		// ok
		return nil
	default:
		return err
	}
}

func (c *Conn) createTopicsV2(request createTopicsRequestV2) (createTopicsResponseV2, error) {
	var response createTopicsResponseV2

	err := c.writeOperation(
		func(deadline time.Time, id int32) error {
			if request.Timeout == 0 {
				now := time.Now()
				deadline = adjustDeadlineForRTT(deadline, now, defaultRTT)
				request.Timeout = milliseconds(deadlineToTimeout(deadline, now))
			}
			return c.writeRequest(createTopicsRequest, v2, id, request)
		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(func() (remain int, err error) {
				return (&response).readFrom(&c.rbuf, size)
			}())
		},
	)
	if err != nil {
		return response, err
	}
	for _, tr := range response.TopicErrors {
		if tr.ErrorCode != 0 {
			return response, Error(tr.ErrorCode)
		}
	}

	return response, nil
}

func (c *Conn) createTopicsV0(request createTopicsRequestV0) (createTopicsResponseV0, error) {
	var response createTopicsResponseV0

	err := c.writeOperation(
		func(deadline time.Time, id int32) error {
			if request.Timeout == 0 {
				now := time.Now()
				deadline = adjustDeadlineForRTT(deadline, now, defaultRTT)
				request.Timeout = milliseconds(deadlineToTimeout(deadline, now))
			}
			return c.writeRequest(createTopicsRequest, v0, id, request)
		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(func() (remain int, err error) {
				return (&response).readFrom(&c.rbuf, size)
			}())
		},
	)
	if err != nil {
		return response, err
	}
	for _, tr := range response.TopicErrors {
		if tr.ErrorCode != 0 {
			return response, Error(tr.ErrorCode)
		}
	}

	return response, nil
}
