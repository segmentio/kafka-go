package kafka

type listGroupsRequestV1 struct {
}

func (t listGroupsRequestV1) size() int32 {
	return 0
}

func (t listGroupsRequestV1) writeTo(wb *writeBuffer) {
}

type ListGroupsResponseGroupV1 struct {
	// GroupID holds the unique group identifier
	GroupID      string
	ProtocolType string
}

func (t ListGroupsResponseGroupV1) size() int32 {
	return sizeofString(t.GroupID) + sizeofString(t.ProtocolType)
}

func (t ListGroupsResponseGroupV1) writeTo(wb *writeBuffer) {
	wb.writeString(t.GroupID)
	wb.writeString(t.ProtocolType)
}

func (t *ListGroupsResponseGroupV1) readFrom(rb *readBuffer) {
	t.GroupID = rb.readString()
	t.ProtocolType = rb.readString()
}

type listGroupsResponseV1 struct {
	// ThrottleTimeMS holds the duration in milliseconds for which the request
	// was throttled due to quota violation (Zero if the request did not violate
	// any quota)
	ThrottleTimeMS int32

	// ErrorCode holds response error code
	ErrorCode int16
	Groups    []ListGroupsResponseGroupV1
}

func (t listGroupsResponseV1) size() int32 {
	return sizeofInt32(t.ThrottleTimeMS) +
		sizeofInt16(t.ErrorCode) +
		sizeofArray(len(t.Groups), func(i int) int32 { return t.Groups[i].size() })
}

func (t listGroupsResponseV1) writeTo(wb *writeBuffer) {
	wb.writeInt32(t.ThrottleTimeMS)
	wb.writeInt16(t.ErrorCode)
	wb.writeArray(len(t.Groups), func(i int) { t.Groups[i].writeTo(wb) })
}

func (t *listGroupsResponseV1) readFrom(rb *readBuffer) {
	t.ThrottleTimeMS = rb.readInt32()
	t.ErrorCode = rb.readInt16()

	rb.readArray(func() {
		group := ListGroupsResponseGroupV1{}
		group.readFrom(rb)
		t.Groups = append(t.Groups, group)
	})
}
