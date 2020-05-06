package listoffsets

import (
	"sort"

	"github.com/segmentio/kafka-go/protocol"
)

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	ReplicaID      int32          `kafka:"min=v1,max=v5"`
	IsolationLevel int8           `kafka:"min=v2,max=v5"`
	Topics         []RequestTopic `kafka:"min=v1,max=v5"`
}

type RequestTopic struct {
	Topic      string             `kafka:"min=v1,max=v5"`
	Partitions []RequestPartition `kafka:"min=v1,max=v5"`
}

type RequestPartition struct {
	Partition          int32 `kafka:"min=v1,max=v5"`
	CurrentLeaderEpoch int32 `kafka:"min=v4,max=v5"`
	Timestamp          int64 `kafka:"min=v1,max=v5"`
	// v0 of the API predates kafka 0.10, and doesn't make much sense to
	// use so we chose not to support it. It had this extra field to limit
	// the number of offsets returned, which has been removed in v1.
	//
	// MaxNumOffsets int32 `kafka:"min=v0,max=v0"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.ListOffsets }

func (r *Request) Map(cluster protocol.Cluster) ([]protocol.Message, protocol.Reducer, error) {
	// Because kafka refuses to answer ListOffsets requests containing multiple
	// entries of unique topic/partition pairs, we submit multiple requests on
	// the wire and merge their results back.
	//
	// The code is a bit verbose but shouldn't be too hard to follow, the idea
	// is to group requests respecting the requirement to have each instance of
	// a topic/partition pair exist only once in each request.
	//
	// Really the idea here is to shield applications from having to deal with
	// the limitation of the kafka server, so they can request any combinations
	// of topic/partition/offsets.
	type topicPartition struct {
		topic     string
		partition int32
	}

	partitionRequests := make(map[topicPartition][]RequestPartition)
	for _, t := range r.Topics {
		for _, p := range t.Partitions {
			key := topicPartition{t.Topic, p.Partition}
			partitionRequests[key] = append(partitionRequests[key], p)
		}
	}

	numRequests := 0
	for _, partitions := range partitionRequests {
		if len(partitions) > numRequests {
			numRequests = len(partitions)
		}
	}

	topicsRequests := make(map[string][]RequestPartition)
	requests := make([]Request, numRequests)
	messages := make([]protocol.Message, numRequests)

	for i := range requests {
		for key, partitions := range partitionRequests {
			if i < len(partitions) {
				topicsRequests[key.topic] = append(topicsRequests[key.topic], partitions[i])
			}
		}

		topics := make([]RequestTopic, 0, len(topicsRequests))
		for topicName, partitions := range topicsRequests {
			topics = append(topics, RequestTopic{
				Topic:      topicName,
				Partitions: partitions,
			})
		}

		for topicName := range topicsRequests {
			delete(topicsRequests, topicName)
		}

		requests[i] = Request{
			ReplicaID:      r.ReplicaID,
			IsolationLevel: r.IsolationLevel,
			Topics:         topics,
		}

		messages[i] = &requests[i]
	}

	return messages, new(Response), nil
}

type Response struct {
	ThrottleTimeMs int32           `kafka:"min=v2,max=v5"`
	Topics         []ResponseTopic `kafka:"min=v1,max=v5"`
}

type ResponseTopic struct {
	Topic      string              `kafka:"min=v1,max=v5"`
	Partitions []ResponsePartition `kafka:"min=v1,max=v5"`
}

type ResponsePartition struct {
	Partition   int32 `kafka:"min=v1,max=v5"`
	ErrorCode   int16 `kafka:"min=v1,max=v5"`
	Timestamp   int64 `kafka:"min=v1,max=v5"`
	Offset      int64 `kafka:"min=v1,max=v5"`
	LeaderEpoch int32 `kafka:"min=v4,max=v5"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.ListOffsets }

func (r *Response) Reduce(requests []protocol.Message, results []interface{}) (protocol.Message, error) {
	topics := make(map[string][]ResponsePartition)
	errors := 0

	for i, res := range results {
		m, err := protocol.Result(res)
		if err != nil {
			for _, t := range requests[i].(*Request).Topics {
				partitions := topics[t.Topic]

				for _, p := range t.Partitions {
					partitions = append(partitions, ResponsePartition{
						Partition:   p.Partition,
						ErrorCode:   -1, // UNKNOWN, can we do better?
						Timestamp:   -1,
						Offset:      -1,
						LeaderEpoch: -1,
					})
				}

				topics[t.Topic] = partitions
			}
			errors++
			continue
		}

		response := m.(*Response)

		if r.ThrottleTimeMs < response.ThrottleTimeMs {
			r.ThrottleTimeMs = response.ThrottleTimeMs
		}

		for _, t := range response.Topics {
			topics[t.Topic] = append(topics[t.Topic], t.Partitions...)
		}
	}

	if errors > 0 && errors == len(results) {
		_, err := protocol.Result(results[0])
		return nil, err
	}

	r.Topics = make([]ResponseTopic, 0, len(topics))

	for topicName, partitions := range topics {
		r.Topics = append(r.Topics, ResponseTopic{
			Topic:      topicName,
			Partitions: partitions,
		})
	}

	sort.Slice(r.Topics, func(i, j int) bool {
		return r.Topics[i].Topic < r.Topics[j].Topic
	})

	for _, t := range r.Topics {
		sort.Slice(t.Partitions, func(i, j int) bool {
			p1 := &t.Partitions[i]
			p2 := &t.Partitions[j]

			if p1.Partition != p2.Partition {
				return p1.Partition < p2.Partition
			}

			return p1.Offset < p2.Offset
		})
	}

	return r, nil
}

var (
	_ protocol.Mapper  = (*Request)(nil)
	_ protocol.Reducer = (*Response)(nil)
)
