package kafka

import (
	"bufio"
	"bytes"
)

// memberGroupAssignments holds MemberID => topic => partitions
type memberGroupAssignments map[string]map[string][]int32

type groupAssignment struct {
	Version  int16
	Topics   map[string][]int32
	UserData []byte
}

func (t groupAssignment) size() int32 {
	sz := sizeofInt16(t.Version) + sizeofInt16(int16(len(t.Topics)))

	for topic, partitions := range t.Topics {
		sz += sizeofString(topic) + sizeofInt32Array(partitions)
	}

	return sz + sizeofBytes(t.UserData)
}

func (t groupAssignment) writeTo(w *bufio.Writer) {
	writeInt16(w, t.Version)
	writeInt16(w, int16(len(t.Topics)))

	for topic, partitions := range t.Topics {
		writeString(w, topic)
		writeInt32Array(w, partitions)
	}

	writeBytes(w, t.UserData)
}

func (t *groupAssignment) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	t.Topics = map[string][]int32{}
	var count int16

	if remain, err = readInt16(r, size, &t.Version); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &count); err != nil {
		return
	}

	for i := 0; i < int(count); i++ {
		var topic string
		var partitions []int32

		if remain, err = readString(r, remain, &topic); err != nil {
			return
		}

		remain, err = readArrayWith(r, remain, func(r *bufio.Reader, size int) (remain int, err error) {
			var partition int32
			remain, err = readInt32(r, size, &partition)
			partitions = append(partitions, partition)
			return
		})

		t.Topics[topic] = partitions
	}

	if remain, err = readBytes(r, remain, &t.UserData); err != nil {
		return
	}

	return
}

func (t groupAssignment) bytes() []byte {
	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	t.writeTo(w)
	w.Flush()
	return buf.Bytes()
}

type syncGroupRequestGroupAssignmentV1 struct {
	// MemberID assigned by the group coordinator
	MemberID string

	// MemberAssignments holds client encoded assignments
	//
	// See consumer groups section of https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
	MemberAssignments []byte
}

func (t syncGroupRequestGroupAssignmentV1) size() int32 {
	return sizeofString(t.MemberID) +
		sizeofBytes(t.MemberAssignments)
}

func (t syncGroupRequestGroupAssignmentV1) writeTo(w *bufio.Writer) {
	writeString(w, t.MemberID)
	writeBytes(w, t.MemberAssignments)
}

type syncGroupRequestV1 struct {
	// GroupID holds the unique group identifier
	GroupID string

	// GenerationID holds the generation of the group.
	GenerationID int32

	// MemberID assigned by the group coordinator
	MemberID string

	GroupAssignments []syncGroupRequestGroupAssignmentV1
}

func (t syncGroupRequestV1) size() int32 {
	return sizeofString(t.GroupID) +
		sizeofInt32(t.GenerationID) +
		sizeofString(t.MemberID) +
		sizeofArray(len(t.GroupAssignments), func(i int) int32 { return t.GroupAssignments[i].size() })
}

func (t syncGroupRequestV1) writeTo(w *bufio.Writer) {
	writeString(w, t.GroupID)
	writeInt32(w, t.GenerationID)
	writeString(w, t.MemberID)
	writeArray(w, len(t.GroupAssignments), func(i int) { t.GroupAssignments[i].writeTo(w) })
}

type syncGroupResponseV1 struct {
	// ThrottleTimeMS holds the duration in milliseconds for which the request
	// was throttled due to quota violation (Zero if the request did not violate
	// any quota)
	ThrottleTimeMS int32

	// ErrorCode holds response error code
	ErrorCode int16

	// MemberAssignments holds client encoded assignments
	//
	// See consumer groups section of https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
	MemberAssignments []byte
}

func (t syncGroupResponseV1) size() int32 {
	return sizeofInt32(t.ThrottleTimeMS) +
		sizeofInt16(t.ErrorCode) +
		sizeofBytes(t.MemberAssignments)
}

func (t syncGroupResponseV1) writeTo(w *bufio.Writer) {
	writeInt32(w, t.ThrottleTimeMS)
	writeInt16(w, t.ErrorCode)
	writeBytes(w, t.MemberAssignments)
}

func (t *syncGroupResponseV1) readFrom(r *bufio.Reader, sz int) (remain int, err error) {
	if remain, err = readInt32(r, sz, &t.ThrottleTimeMS); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &t.ErrorCode); err != nil {
		return
	}
	if remain, err = readBytes(r, remain, &t.MemberAssignments); err != nil {
		return
	}
	return
}
