package describegroups

import (
	"github.com/segmentio/kafka-go/protocol"
)

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	Groups                      []string `kafka:"min=v0,max=v4"`
	IncludeAuthorizedOperations bool     `kafka:"min=v3,max=v4"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.DescribeGroups }

func (r *Request) Group() string {
	// TODO: Handle other groups too (via splitter).
	return r.Groups[0]
}

type Response struct {
	ThrottleTimeMs int32           `kafka:"min=v1,max=v4"`
	Groups         []ResponseGroup `kafka:"min=v0,max=v4"`
}

type ResponseGroup struct {
	ErrorCode            int16                 `kafka:"min=v0,max=v4"`
	GroupID              string                `kafka:"min=v0,max=v4"`
	GroupState           string                `kafka:"min=v0,max=v4"`
	ProtocolType         string                `kafka:"min=v0,max=v4"`
	ProtocolData         string                `kafka:"min=v0,max=v4"`
	Members              []ResponseGroupMember `kafka:"min=v0,max=v4"`
	AuthorizedOperations int32                 `kafka:"min=v3,max=v4"`
}

type ResponseGroupMember struct {
	MemberID         string `kafka:"min=v0,max=v4"`
	GroupInstanceID  string `kafka:"min=v4,max=v4,nullable"`
	ClientID         string `kafka:"min=v0,max=v4"`
	ClientHost       string `kafka:"min=v0,max=v4"`
	MemberMetadata   []byte `kafka:"min=v0,max=v4"`
	MemberAssignment []byte `kafka:"min=v0,max=v4"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.DescribeGroups }
