package addpartitionstotxn

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	// We need at least one tagged field to indicate that this is a "flexible" message
	// type.
	_ struct{} `kafka:"min=v3,max=v5,tag"`

	Transactions []RequestTransaction `kafka:"min=v4,max=v5"`

	TransactionalID string         `kafka:"min=v0,max=v2|min=v3,max=v3,compact"`
	ProducerID      int64          `kafka:"min=v0,max=v3"`
	ProducerEpoch   int16          `kafka:"min=v0,max=v3"`
	Topics          []RequestTopic `kafka:"min=v0,max=v3"`
	VerifyOnly      bool           `kafka:"-"`
}

type RequestTransaction struct {
	_ struct{} `kafka:"min=v4,max=v5,tag"`

	TransactionalID string         `kafka:"min=v4,max=v5,compact"`
	ProducerID      int64          `kafka:"min=v4,max=v5"`
	ProducerEpoch   int16          `kafka:"min=v4,max=v5"`
	VerifyOnly      bool           `kafka:"min=v4,max=v5"`
	Topics          []RequestTopic `kafka:"min=v4,max=v5"`
}

type RequestTopic struct {
	// We need at least one tagged field to indicate that this is a "flexible" message
	// type.
	_ struct{} `kafka:"min=v3,max=v5,tag"`

	Name       string  `kafka:"min=v0,max=v2|min=v3,max=v5,compact"`
	Partitions []int32 `kafka:"min=v0,max=v5"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.AddPartitionsToTxn }

func (r *Request) Transaction() string {
	if r.TransactionalID != "" {
		return r.TransactionalID
	}
	if len(r.Transactions) > 0 {
		return r.Transactions[0].TransactionalID
	}
	return ""
}

func (r *Request) Prepare(apiVersion int16) {
	if apiVersion >= 4 {
		if len(r.Transactions) == 0 && r.TransactionalID != "" {
			r.Transactions = []RequestTransaction{
				{
					TransactionalID: r.TransactionalID,
					ProducerID:      r.ProducerID,
					ProducerEpoch:   r.ProducerEpoch,
					VerifyOnly:      r.VerifyOnly,
					Topics:          r.Topics,
				},
			}
		}
	} else {
		if len(r.Transactions) > 0 && r.TransactionalID == "" {
			r.TransactionalID = r.Transactions[0].TransactionalID
			r.ProducerID = r.Transactions[0].ProducerID
			r.ProducerEpoch = r.Transactions[0].ProducerEpoch
			r.Topics = r.Transactions[0].Topics
			r.VerifyOnly = r.Transactions[0].VerifyOnly
		}
	}
}

var (
	_ protocol.TransactionalMessage = (*Request)(nil)
	_ protocol.PreparedMessage      = (*Request)(nil)
)

type Response struct {
	// We need at least one tagged field to indicate that this is a "flexible" message
	// type.
	_ struct{} `kafka:"min=v3,max=v5,tag"`

	ThrottleTimeMs       int32                       `kafka:"min=v0,max=v5"`
	ErrorCode            int16                       `kafka:"min=v4,max=v5"`
	ResultsByTransaction []ResponseTransactionResult `kafka:"min=v4,max=v5"`
	Results              []ResponseResult            `kafka:"min=v0,max=v3"`
}

type ResponseTransactionResult struct {
	_ struct{} `kafka:"min=v4,max=v5,tag"`

	TransactionalID string           `kafka:"min=v4,max=v5,compact"`
	TopicResults    []ResponseResult `kafka:"min=v4,max=v5"`
}

type ResponseResult struct {
	// We need at least one tagged field to indicate that this is a "flexible" message
	// type.
	_ struct{} `kafka:"min=v3,max=v5,tag"`

	Name    string              `kafka:"min=v0,max=v2|min=v3,max=v5,compact"`
	Results []ResponsePartition `kafka:"min=v0,max=v5"`
}

type ResponsePartition struct {
	// We need at least one tagged field to indicate that this is a "flexible" message
	// type.
	_ struct{} `kafka:"min=v3,max=v5,tag"`

	PartitionIndex int32 `kafka:"min=v0,max=v5"`
	ErrorCode      int16 `kafka:"min=v0,max=v5"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.AddPartitionsToTxn }
