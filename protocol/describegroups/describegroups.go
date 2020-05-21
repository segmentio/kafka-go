package describegroups

import (
	"github.com/segmentio/kafka-go/protocol"
)

type State string

const (
	Dead               State = "Dead"
	Stable             State = "Stable"
	AwaitingSync       State = "AwaitingSync"
	PreparingRebalance State = "PreparingRebalance"
)

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	GroupIDs []string `kafka:"min=v0,max=v0"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.DescribeGroups }

func (r *Request) Group() string {
	if len(r.GroupIDs) != 1 {
		return ""
	}
	return r.GroupIDs[0]
}

func (r *Request) Map(cluster protocol.Cluster) ([]protocol.Message, protocol.Reducer, error) {
	requests := make([]protocol.Message, len(r.GroupIDs))
	for i, group := range r.GroupIDs {
		requests[i] = &Request{GroupIDs: []string{group}}
	}
	return requests, new(Response), nil
}

type Response struct {
	Groups []Group `kafka:"min=v0,max=v0"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.DescribeGroups }

func (r *Response) Reduce(requests []protocol.Message, results []interface{}) (protocol.Message, error) {
	var reduced Response
	for _, result := range results {
		m, err := protocol.Result(result)
		if err != nil {
			return nil, err // todo : handle better?
		}
		reduced.Groups = append(reduced.Groups, m.(*Response).Groups...)
	}
	return &reduced, nil
}

type Group struct {
	ErrorCode    int16         `kafka:"min=v0,max=v0"`
	GroupID      string        `kafka:"min=v0,max=v0"`
	State        State         `kafka:"min=v0,max=v0"`
	ProtocolType string        `kafka:"min=v0,max=v0"`
	Protocol     string        `kafka:"min=v0,max=v0"`
	Members      []GroupMember `kafka:"min=v0,max=v0"`
}

type GroupMember struct {
	MemberID         string `kafka:"min=v0,max=v0"`
	ClientID         string `kafka:"min=v0,max=v0"`
	ClientHost       string `kafka:"min=v0,max=v0"`
	MemberMetadata   []byte `kafka:"min=v0,max=v0"`
	MemberAssignment []byte `kafka:"min=v0,max=v0"`
}

// ConsumerGroupMetadata is the structured representation of
// GroupMember.MemberMetadata when the group is using Kafka's consumer group
// protocol.  This is the usual case and will be indicated by the Group's
// ProtocolType being set to 'consumer'.
//
// After checking the protocol type, this struct can be unmarshalled using
// kafka.Unmarshal.
type ConsumerGroupMetadata struct {
	Version  int16
	Topics   []string
	UserData []byte
}

// ConsumerGroupAssignment is the structured representation of
// GroupMember.MemberAssignment when the group is using Kafka's consumer group
// protocol.  This is the usual case and will be indicated by the Group's
// ProtocolType being set to 'consumer'.
//
// After checking the protocol type, this struct can be unmarshalled using
// kafka.Unmarshal.
type ConsumerGroupAssignment struct {
	Version  int16
	Topics   []ConsumerGroupTopic
	UserData []byte
}

type ConsumerGroupTopic struct {
	Topic      string
	Partitions []int32
}
