package kafka

// See http://kafka.apache.org/protocol.html#The_Messages_DescribeGroups
type describeGroupsRequestV0 struct {
	// List of groupIds to request metadata for (an empty groupId array
	// will return empty group metadata).
	GroupIDs []string
}

func (t describeGroupsRequestV0) size() int32 {
	return sizeofStringArray(t.GroupIDs)
}

func (t describeGroupsRequestV0) writeTo(wb *writeBuffer) {
	wb.writeStringArray(t.GroupIDs)
}

type describeGroupsResponseMemberV0 struct {
	// MemberID assigned by the group coordinator
	MemberID string

	// ClientID used in the member's latest join group request
	ClientID string

	// ClientHost used in the request session corresponding to the member's
	// join group.
	ClientHost string

	// MemberMetadata the metadata corresponding to the current group protocol
	// in use (will only be present if the group is stable).
	MemberMetadata []byte

	// MemberAssignments provided by the group leader (will only be present if
	// the group is stable).
	//
	// See consumer groups section of https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
	MemberAssignments []byte
}

func (t describeGroupsResponseMemberV0) size() int32 {
	return sizeofString(t.MemberID) +
		sizeofString(t.ClientID) +
		sizeofString(t.ClientHost) +
		sizeofBytes(t.MemberMetadata) +
		sizeofBytes(t.MemberAssignments)
}

func (t describeGroupsResponseMemberV0) writeTo(wb *writeBuffer) {
	wb.writeString(t.MemberID)
	wb.writeString(t.ClientID)
	wb.writeString(t.ClientHost)
	wb.writeBytes(t.MemberMetadata)
	wb.writeBytes(t.MemberAssignments)
}

func (t *describeGroupsResponseMemberV0) readFrom(rb *readBuffer) {
	t.MemberID = rb.readString()
	t.ClientID = rb.readString()
	t.ClientHost = rb.readString()
	t.MemberMetadata = rb.readBytes()
	t.MemberAssignments = rb.readBytes()
}

type describeGroupsResponseGroupV0 struct {
	// ErrorCode holds response error code
	ErrorCode int16

	// GroupID holds the unique group identifier
	GroupID string

	// State holds current state of the group (one of: Dead, Stable, AwaitingSync,
	// PreparingRebalance, or empty if there is no active group)
	State string

	// ProtocolType holds the current group protocol type (will be empty if there is
	// no active group)
	ProtocolType string

	// Protocol holds the current group protocol (only provided if the group is Stable)
	Protocol string

	// Members contains the current group members (only provided if the group is not Dead)
	Members []describeGroupsResponseMemberV0
}

func (t describeGroupsResponseGroupV0) size() int32 {
	return sizeofInt16(t.ErrorCode) +
		sizeofString(t.GroupID) +
		sizeofString(t.State) +
		sizeofString(t.ProtocolType) +
		sizeofString(t.Protocol) +
		sizeofArray(len(t.Members), func(i int) int32 { return t.Members[i].size() })
}

func (t describeGroupsResponseGroupV0) writeTo(wb *writeBuffer) {
	wb.writeInt16(t.ErrorCode)
	wb.writeString(t.GroupID)
	wb.writeString(t.State)
	wb.writeString(t.ProtocolType)
	wb.writeString(t.Protocol)
	wb.writeArray(len(t.Members), func(i int) { t.Members[i].writeTo(wb) })
}

func (t *describeGroupsResponseGroupV0) readFrom(rb *readBuffer) {
	t.ErrorCode = rb.readInt16()
	t.GroupID = rb.readString()
	t.State = rb.readString()
	t.ProtocolType = rb.readString()
	t.Protocol = rb.readString()

	rb.readArray(func() {
		member := describeGroupsResponseMemberV0{}
		member.readFrom(rb)
		t.Members = append(t.Members, member)
	})
}

type describeGroupsResponseV0 struct {
	// Groups holds selected group information
	Groups []describeGroupsResponseGroupV0
}

func (t describeGroupsResponseV0) size() int32 {
	return sizeofArray(len(t.Groups), func(i int) int32 { return t.Groups[i].size() })
}

func (t describeGroupsResponseV0) writeTo(wb *writeBuffer) {
	wb.writeArray(len(t.Groups), func(i int) { t.Groups[i].writeTo(wb) })
}

func (t *describeGroupsResponseV0) readFrom(rb *readBuffer) {
	rb.readArray(func() {
		group := describeGroupsResponseGroupV0{}
		group.readFrom(rb)
		t.Groups = append(t.Groups, group)
	})
}
