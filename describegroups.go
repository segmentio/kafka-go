package kafka

import (
	"context"
	"net"

	"github.com/segmentio/kafka-go/protocol/consumer"
	"github.com/segmentio/kafka-go/protocol/describegroups"
)

// DescribeGroupsRequest is a request to the DescribeGroups API.
type DescribeGroupsRequest struct {
	// Addr is the address of the kafka broker to send the request to.
	Addr net.Addr

	// GroupIDs is a slice of groups to get details for.
	GroupIDs []string
}

// DescribeGroupsResponse is a response from the DescribeGroups API.
type DescribeGroupsResponse struct {
	// Groups is a slice of details for the requested groups.
	Groups []DescribeGroupsResponseGroup
}

// DescribeGroupsResponseGroup contains the response details for a single group.
type DescribeGroupsResponseGroup struct {
	// Error is set to a non-nil value if there was an error fetching the details
	// for this group.
	Error error

	// GroupID is the ID of the group.
	GroupID string

	// GroupState is a description of the group state.
	GroupState string

	// Members contains details about each member of the group.
	Members []DescribeGroupsResponseMember
}

// MemberInfo represents the membership information for a single group member.
type DescribeGroupsResponseMember struct {
	// MemberID is the ID of the group member.
	MemberID string

	// ClientID is the ID of the client that the group member is using.
	ClientID string

	// ClientHost is the host of the client that the group member is connecting from.
	ClientHost string

	// MemberMetadata contains metadata about this group member.
	MemberMetadata DescribeGroupsResponseMemberMetadata

	// MemberAssignments contains the topic partitions that this member is assigned to.
	MemberAssignments DescribeGroupsResponseAssignments
}

// GroupMemberMetadata stores metadata associated with a group member.
type DescribeGroupsResponseMemberMetadata struct {
	// Version is the version of the metadata.
	Version int

	// Topics is the list of topics that the member is assigned to.
	Topics []string

	// UserData is the user data for the member.
	UserData []byte

	// OwnedPartitions contains the partitions owned by this group member; only set if
	// consumers are using a cooperative rebalancing assignor protocol.
	OwnedPartitions []DescribeGroupsResponseMemberMetadataOwnedPartition
}

type DescribeGroupsResponseMemberMetadataOwnedPartition struct {
	// Topic is the name of the topic.
	Topic string

	// Partitions is the partitions that are owned by the group in the topic.
	Partitions []int
}

// GroupMemberAssignmentsInfo stores the topic partition assignment data for a group member.
type DescribeGroupsResponseAssignments struct {
	// Version is the version of the assignments data.
	Version int

	// Topics contains the details of the partition assignments for each topic.
	Topics []GroupMemberTopic

	// UserData is the user data for the member.
	UserData []byte
}

// GroupMemberTopic is a mapping from a topic to a list of partitions in the topic. It is used
// to represent the topic partitions that have been assigned to a group member.
type GroupMemberTopic struct {
	// Topic is the name of the topic.
	Topic string

	// Partitions is a slice of partition IDs that this member is assigned to in the topic.
	Partitions []int
}

// DescribeGroups calls the Kafka DescribeGroups API to get information about one or more
// consumer groups. See https://kafka.apache.org/protocol#The_Messages_DescribeGroups for
// more information.
func (c *Client) DescribeGroups(
	ctx context.Context,
	req *DescribeGroupsRequest,
) (*DescribeGroupsResponse, error) {
	protoResp, err := c.roundTrip(
		ctx,
		req.Addr,
		&describegroups.Request{
			Groups: req.GroupIDs,
		},
	)
	if err != nil {
		return nil, err
	}
	apiResp := protoResp.(*describegroups.Response)
	resp := &DescribeGroupsResponse{}

	for _, apiGroup := range apiResp.Groups {
		group := DescribeGroupsResponseGroup{
			Error:      makeError(apiGroup.ErrorCode, ""),
			GroupID:    apiGroup.GroupID,
			GroupState: apiGroup.GroupState,
		}

		for _, member := range apiGroup.Members {
			decodedMetadata, err := decodeMemberMetadata(member.MemberMetadata)
			if err != nil {
				return nil, err
			}
			decodedAssignments, err := decodeMemberAssignments(member.MemberAssignment)
			if err != nil {
				return nil, err
			}

			group.Members = append(group.Members, DescribeGroupsResponseMember{
				MemberID:          member.MemberID,
				ClientID:          member.ClientID,
				ClientHost:        member.ClientHost,
				MemberAssignments: decodedAssignments,
				MemberMetadata:    decodedMetadata,
			})
		}
		resp.Groups = append(resp.Groups, group)
	}

	return resp, nil
}

// decodeMemberMetadata converts raw metadata bytes to a
// DescribeGroupsResponseMemberMetadata struct.
//
// See https://github.com/apache/kafka/blob/2.4/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerProtocol.java#L49
// for protocol details.
func decodeMemberMetadata(rawMetadata []byte) (DescribeGroupsResponseMemberMetadata, error) {
	mm := DescribeGroupsResponseMemberMetadata{}

	if len(rawMetadata) == 0 {
		return mm, nil
	}

	var sub consumer.Subscription
	err := sub.FromBytes(rawMetadata)
	if err != nil {
		return mm, err
	}

	mm.Version = int(sub.Version)
	mm.Topics = sub.Topics
	mm.UserData = sub.UserData
	mm.OwnedPartitions = make([]DescribeGroupsResponseMemberMetadataOwnedPartition, len(sub.OwnedPartitions))
	for i, op := range sub.OwnedPartitions {
		mm.OwnedPartitions[i] = DescribeGroupsResponseMemberMetadataOwnedPartition{
			Topic:      op.Topic,
			Partitions: make([]int, len(op.Partitions)),
		}
		for j, part := range op.Partitions {
			mm.OwnedPartitions[i].Partitions[j] = int(part)
		}
	}

	return mm, nil
}

// decodeMemberAssignments converts raw assignment bytes to a DescribeGroupsResponseAssignments
// struct.
//
// See https://github.com/apache/kafka/blob/2.4/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerProtocol.java#L49
// for protocol details.
func decodeMemberAssignments(rawAssignments []byte) (DescribeGroupsResponseAssignments, error) {
	ma := DescribeGroupsResponseAssignments{}

	if len(rawAssignments) == 0 {
		return ma, nil
	}

	var assignment consumer.Assignment
	err := assignment.FromBytes(rawAssignments)
	if err != nil {
		return ma, err
	}

	ma.Version = int(assignment.Version)
	ma.UserData = assignment.UserData
	ma.Topics = make([]GroupMemberTopic, len(assignment.AssignedPartitions))
	for i, topic := range assignment.AssignedPartitions {
		ma.Topics[i] = GroupMemberTopic{
			Topic:      topic.Topic,
			Partitions: make([]int, len(topic.Partitions)),
		}
		for j, part := range topic.Partitions {
			ma.Topics[i].Partitions[j] = int(part)
		}
	}

	return ma, nil
}
