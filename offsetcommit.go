package kafka

import "bufio"

type offsetCommitRequestV3Partition struct {
	// Partition ID
	Partition int32

	// Offset to be committed
	Offset int64

	// Metadata holds any associated metadata the client wants to keep
	Metadata string
}

func (t offsetCommitRequestV3Partition) size() int32 {
	return sizeofInt32(t.Partition) +
		sizeofInt64(t.Offset) +
		sizeofString(t.Metadata)
}

func (t offsetCommitRequestV3Partition) writeTo(w *bufio.Writer) {
	writeInt32(w, t.Partition)
	writeInt64(w, t.Offset)
	writeString(w, t.Metadata)
}

type offsetCommitRequestV3Topic struct {
	// Topic name
	Topic string

	// Partitions to commit offsets
	Partitions []offsetCommitRequestV3Partition
}

func (t offsetCommitRequestV3Topic) size() int32 {
	return sizeofString(t.Topic) +
		sizeofArray(len(t.Partitions), func(i int) int32 { return t.Partitions[i].size() })
}

func (t offsetCommitRequestV3Topic) writeTo(w *bufio.Writer) {
	writeString(w, t.Topic)
	writeArray(w, len(t.Partitions), func(i int) { t.Partitions[i].writeTo(w) })
}

type offsetCommitRequestV3 struct {
	// GroupID holds the unique group identifier
	GroupID string

	// GenerationID holds the generation of the group.
	GenerationID int32

	// MemberID assigned by the group coordinator
	MemberID string

	// RetentionTime holds the time period in ms to retain the offset.
	RetentionTime int64

	// Topics to commit offsets
	Topics []offsetCommitRequestV3Topic
}

func (t offsetCommitRequestV3) size() int32 {
	return sizeofString(t.GroupID) +
		sizeofInt32(t.GenerationID) +
		sizeofString(t.MemberID) +
		sizeofInt64(t.RetentionTime) +
		sizeofArray(len(t.Topics), func(i int) int32 { return t.Topics[i].size() })
}

func (t offsetCommitRequestV3) writeTo(w *bufio.Writer) {
	writeString(w, t.GroupID)
	writeInt32(w, t.GenerationID)
	writeString(w, t.MemberID)
	writeInt64(w, t.RetentionTime)
	writeArray(w, len(t.Topics), func(i int) { t.Topics[i].writeTo(w) })
}

type offsetCommitResponseV3PartitionResponse struct {
	Partition int32

	// ErrorCode holds response error code
	ErrorCode int16
}

func (t offsetCommitResponseV3PartitionResponse) size() int32 {
	return sizeofInt32(t.Partition) +
		sizeofInt16(t.ErrorCode)
}

func (t offsetCommitResponseV3PartitionResponse) writeTo(w *bufio.Writer) {
	writeInt32(w, t.Partition)
	writeInt16(w, t.ErrorCode)
}

func (t *offsetCommitResponseV3PartitionResponse) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt32(r, size, &t.Partition); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &t.ErrorCode); err != nil {
		return
	}
	return
}

type offsetCommitResponseV3Response struct {
	Topic              string
	PartitionResponses []offsetCommitResponseV3PartitionResponse
}

func (t offsetCommitResponseV3Response) size() int32 {
	return sizeofString(t.Topic) +
		sizeofArray(len(t.PartitionResponses), func(i int) int32 { return t.PartitionResponses[i].size() })
}

func (t offsetCommitResponseV3Response) writeTo(w *bufio.Writer) {
	writeString(w, t.Topic)
	writeArray(w, len(t.PartitionResponses), func(i int) { t.PartitionResponses[i].writeTo(w) })
}

func (t *offsetCommitResponseV3Response) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readString(r, size, &t.Topic); err != nil {
		return
	}

	fn := func(r *bufio.Reader, withSize int) (fnRemain int, fnErr error) {
		item := offsetCommitResponseV3PartitionResponse{}
		if fnRemain, fnErr = (&item).readFrom(r, withSize); fnErr != nil {
			return
		}
		t.PartitionResponses = append(t.PartitionResponses, item)
		return
	}
	if remain, err = readArrayWith(r, remain, fn); err != nil {
		return
	}

	return
}

type offsetCommitResponseV3 struct {
	// ThrottleTimeMS holds the duration in milliseconds for which the request
	// was throttled due to quota violation (Zero if the request did not violate
	// any quota)
	ThrottleTimeMS int32
	Responses      []offsetCommitResponseV3Response
}

func (t offsetCommitResponseV3) size() int32 {
	return sizeofInt32(t.ThrottleTimeMS) +
		sizeofArray(len(t.Responses), func(i int) int32 { return t.Responses[i].size() })
}

func (t offsetCommitResponseV3) writeTo(w *bufio.Writer) {
	writeInt32(w, t.ThrottleTimeMS)
	writeArray(w, len(t.Responses), func(i int) { t.Responses[i].writeTo(w) })
}

func (t *offsetCommitResponseV3) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt32(r, size, &t.ThrottleTimeMS); err != nil {
		return
	}

	fn := func(r *bufio.Reader, withSize int) (fnRemain int, fnErr error) {
		item := offsetCommitResponseV3Response{}
		if fnRemain, fnErr = (&item).readFrom(r, withSize); fnErr != nil {
			return
		}
		t.Responses = append(t.Responses, item)
		return
	}
	if remain, err = readArrayWith(r, remain, fn); err != nil {
		return
	}

	return
}
