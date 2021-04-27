package offsetcommit

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	GroupID           string         `kafka:"min=v0,max=v2"`
	GroupGenerationID int32          `kafka:"min=v1,max=v2"`
	MemberID          string         `kafka:"min=v1,max=v2"`
	RetentionTime     int64          `kafka:"min=v2,max=v2"`
	Topics            []RequestTopic `kafka:"min=v0,max=v2"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.OffsetCommit }

func (r *Request) Group() string { return r.GroupID }

type RequestTopic struct {
	Name       string             `kafka:"min=v0,max=v2"`
	Partitions []RequestPartition `kafka:"min=v0,max=v2"`
}

type RequestPartition struct {
	Partition int32  `kafka:"min=v0,max=v2"`
	Offset    int64  `kafka:"min=v0,max=v2"`
	Timestamp int64  `kafka:"min=v1,max=v1"`
	Metadata  string `kafka:"min=v0,max=v2,nullable"`
}

var (
	_ protocol.GroupMessage = (*Request)(nil)
)

type Response struct {
	Topics []ResponseTopic `kafka:"min=v0,max=v2"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.OffsetCommit }

type ResponseTopic struct {
	Name       string              `kafka:"min=v0,max=v2"`
	Partitions []ResponsePartition `kafka:"min=v0,max=v2"`
}

type ResponsePartition struct {
	Partition int32 `kafka:"min=v0,max=v2"`
	ErrorCode int16 `kafka:"min=v0,max=v2"`
}
