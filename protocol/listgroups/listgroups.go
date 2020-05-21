package listgroups

import (
	"github.com/segmentio/kafka-go/protocol"
)

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	_      struct{} `kafka:"min=v0,max=v0"`
	broker protocol.Broker
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.ListGroups }

func (r *Request) Broker(cluster protocol.Cluster) (protocol.Broker, error) {
	if r.broker.Host != "" {
		return r.broker, nil
	}
	// if this isn't a request from Map(), then execute against any broker.
	return protocol.Broker{ID: -1}, nil
}

func (r *Request) Map(cluster protocol.Cluster) ([]protocol.Message, protocol.Reducer, error) {
	requests := make([]protocol.Message, 0, len(cluster.Brokers))
	for _, broker := range cluster.Brokers {
		requests = append(requests, &Request{broker: broker})
	}
	return requests, new(Response), nil
}

type Response struct {
	ErrorCode int16   `kafka:"min=v0,max=v0"`
	Groups    []Group `kafka:"min=v0,max=v0"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.ListGroups }

type Group struct {
	GroupID      string `kafka:"min=v0,max=v0"`
	ProtocolType string `kafka:"min=v0,max=v0"`
}

func (r *Response) Reduce(requests []protocol.Message, results []interface{}) (protocol.Message, error) {
	var reduced Response
	for _, result := range results {
		m, err := protocol.Result(result)
		if err != nil {
			return nil, err
		}
		resp := m.(*Response)
		if resp.ErrorCode != 0 {
			// todo : what to do here?
		}
		reduced.Groups = append(reduced.Groups, resp.Groups...)
	}
	return &reduced, nil
}
