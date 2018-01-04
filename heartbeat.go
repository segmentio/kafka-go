package kafka

import "bufio"

type heartbeatRequestV1 struct {
	// GroupID holds the unique group identifier
	GroupID string

	// GenerationID holds the generation of the group.
	GenerationID int32

	// MemberID assigned by the group coordinator
	MemberID string
}

func (t heartbeatRequestV1) size() int32 {
	return sizeofString(t.GroupID) +
		sizeofInt32(t.GenerationID) +
		sizeofString(t.MemberID)
}

func (t heartbeatRequestV1) writeTo(w *bufio.Writer) {
	writeString(w, t.GroupID)
	writeInt32(w, t.GenerationID)
	writeString(w, t.MemberID)
}

type heartbeatResponseV1 struct {
	// ThrottleTimeMS holds the duration in milliseconds for which the request
	// was throttled due to quota violation (Zero if the request did not violate
	// any quota)
	ThrottleTimeMS int32

	// ErrorCode holds response error code
	ErrorCode int16
}

func (t heartbeatResponseV1) size() int32 {
	return sizeofInt32(t.ThrottleTimeMS) +
		sizeofInt16(t.ErrorCode)
}

func (t heartbeatResponseV1) writeTo(w *bufio.Writer) {
	writeInt32(w, t.ThrottleTimeMS)
	writeInt16(w, t.ErrorCode)
}

func (t *heartbeatResponseV1) readFrom(r *bufio.Reader, sz int) (remain int, err error) {
	if remain, err = readInt32(r, sz, &t.ThrottleTimeMS); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &t.ErrorCode); err != nil {
		return
	}
	return
}
