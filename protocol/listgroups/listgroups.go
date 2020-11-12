package listgroups

import (
	"errors"
	"log"

	"github.com/segmentio/kafka-go/protocol"
)

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	brokerID int32
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.ListGroups }

func (r *Request) Broker(cluster protocol.Cluster) (protocol.Broker, error) {
	log.Printf("Getting id for broker %d\n", r.brokerID)
	return cluster.Brokers[r.brokerID], nil
}

func (r *Request) Split(cluster protocol.Cluster) (
	[]protocol.Message,
	protocol.Merger,
	error,
) {
	messages := []protocol.Message{}

	for _, broker := range cluster.Brokers {
		messages = append(messages, &Request{broker.ID})
	}

	log.Printf("Spliting request into %d subrequests\n", len(messages))

	return nil, new(Response), nil
}

type Response struct {
	ThrottleTimeMs int32           `kafka:"min=v1,max=v2"`
	ErrorCode      int16           `kafka:"min=v0,max=v2"`
	Groups         []ResponseGroup `kafka:"min=v0,max=v2"`
}

type ResponseGroup struct {
	GroupID      string `kafka:"min=v0,max=v2"`
	ProtocolType string `kafka:"min=v0,max=v2"`

	// Use this to store which broker returned the response
	BrokerID int32 `kafka:"-"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.ListGroups }

func (r *Response) Merge(requests []protocol.Message, results []interface{}) (
	protocol.Message,
	error,
) {
	response := &Response{}

	for r, result := range results {
		brokerResp, ok := result.(*Response)
		if !ok {
			return nil, errors.New("Unexpected response type")
		}

		respGroups := []ResponseGroup{}

		for _, brokerResp := range brokerResp.Groups {
			respGroups = append(
				respGroups,
				ResponseGroup{
					GroupID:      brokerResp.GroupID,
					ProtocolType: brokerResp.ProtocolType,
					BrokerID:     requests[r].(*Request).brokerID,
				},
			)
		}

		response.Groups = append(response.Groups, brokerResp.Groups...)
	}

	return response, nil
}
