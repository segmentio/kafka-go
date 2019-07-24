package kafka

type leaveGroupRequestV0 struct {
	// GroupID holds the unique group identifier
	GroupID string

	// MemberID assigned by the group coordinator or the zero string if joining
	// for the first time.
	MemberID string
}

func (t leaveGroupRequestV0) size() int32 {
	return sizeofString(t.GroupID) + sizeofString(t.MemberID)
}

func (t leaveGroupRequestV0) writeTo(wb *writeBuffer) {
	wb.writeString(t.GroupID)
	wb.writeString(t.MemberID)
}

func (t *leaveGroupRequestV0) readFrom(rb *readBuffer) {
	t.GroupID = rb.readString()
	t.MemberID = rb.readString()
}

type leaveGroupResponseV0 struct {
	// ErrorCode holds response error code
	ErrorCode int16
}

func (t leaveGroupResponseV0) size() int32 {
	return sizeofInt16(t.ErrorCode)
}

func (t leaveGroupResponseV0) writeTo(wb *writeBuffer) {
	wb.writeInt16(t.ErrorCode)
}

func (t *leaveGroupResponseV0) readFrom(rb *readBuffer) {
	t.ErrorCode = rb.readInt16()
}
