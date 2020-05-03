package kafka

import (
	"bufio"
	"context"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/segmentio/kafka-go/protocol/listoffsets"
)

// OffsetRequest represents a request to retrieve a single partition offset.
type OffsetRequest struct {
	Partition int
	Timestamp int64
}

// FirstOffsetOf constructs an OffsetRequest which asks for the first offset of
// the parition given as argument.
func FirstOffsetOf(partition int) OffsetRequest {
	return OffsetRequest{Partition: partition, Timestamp: FirstOffset}
}

// LastOffsetOf constructs an OffsetRequest which asks for the last offset of
// the partition given as argument.
func LastOffsetOf(partition int) OffsetRequest {
	return OffsetRequest{Partition: partition, Timestamp: LastOffset}
}

// TimeOffsetOf constructs an OffsetRequest which asks for a partition offset
// at a given time.
func TimeOffsetOf(partition int, at time.Time) OffsetRequest {
	return OffsetRequest{Partition: partition, Timestamp: timestamp(at)}
}

// PartitionOffsets carries information about offsets available in a topic
// partition.
type PartitionOffset struct {
	Partition   int
	FirstOffset int64
	LastOffset  int64
	Offsets     map[int64]time.Time
	Error       error
}

// ListOffsetsRequest represents a request sent to a kafka broker to list of the
// offsets of topic partitions.
type ListOffsetsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	// A mapping of topic names to list of partitions that the program wishes to
	// get the offsets for.
	Topics map[string][]OffsetRequest

	// The isolation level for the request.
	//
	// Defaults to ReadUncommitted.
	//
	// This field requires the kafka broker to support the ListOffsets API in
	// version 2 or above (otherwise the value is ignored).
	IsolationLevel IsolationLevel
}

// ListOffsetsResponse represents a response from a kafka broker to a offset
// listing request.
type ListOffsetsResponse struct {
	// The amount of time that the broker throttled the request.
	Throttle time.Duration

	// Mappings of topics names to partition offsets, there will be one entry
	// for each topic in the request.
	Topics map[string][]PartitionOffset
}

// ListOffsets sends an offset request to a kafka broker and returns the
// response.
func (c *Client) ListOffsets(ctx context.Context, req *ListOffsetsRequest) (*ListOffsetsResponse, error) {
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
		partition int
	}

	type topicResponse struct {
		topic     string
		partition int
		index     int
		offset    int64
		time      time.Time
		throttle  time.Duration
		err       error
	}

	var (
		partitionRequests = make(map[topicPartition][]int64)
		topicRequests     = make(map[string][]listoffsets.RequestPartition)
		responses         = make(chan topicResponse)
		numRequests       int
		waitGroup         sync.WaitGroup
	)

	for topicName, offsetRequests := range req.Topics {
		for _, offsetRequest := range offsetRequests {
			key := topicPartition{topicName, offsetRequest.Partition}
			partitionRequests[key] = append(partitionRequests[key], offsetRequest.Timestamp)
		}
	}

	for _, timestamps := range partitionRequests {
		if len(timestamps) > numRequests {
			numRequests = len(timestamps)
		}
	}

	for i := 0; i < numRequests; i++ {
		for key, timestamps := range partitionRequests {
			topicRequests[key.topic] = append(topicRequests[key.topic], listoffsets.RequestPartition{
				Partition:          int32(key.partition),
				CurrentLeaderEpoch: -1,
				Timestamp:          timestamps[i],
			})
		}

		r := &listoffsets.Request{
			ReplicaID:      -1,
			IsolationLevel: int8(req.IsolationLevel),
			Topics:         make([]listoffsets.RequestTopic, 0, len(topicRequests)),
		}

		for topicName, partitions := range topicRequests {
			r.Topics = append(r.Topics, listoffsets.RequestTopic{
				Topic:      topicName,
				Partitions: partitions,
			})
		}

		for topicName := range topicRequests {
			delete(topicRequests, topicName)
		}

		waitGroup.Add(1)
		go func(index int, addr net.Addr, req *listoffsets.Request) {
			defer waitGroup.Done()
			m, err := c.roundTrip(ctx, addr, req)

			if err != nil {
				for _, t := range req.Topics {
					for _, p := range t.Partitions {
						responses <- topicResponse{
							topic:     t.Topic,
							partition: int(p.Partition),
							index:     index,
							offset:    -1,
							err:       err,
						}
					}
				}
				return
			}

			res := m.(*listoffsets.Response)
			for _, t := range res.Topics {
				for _, p := range t.Partitions {
					responses <- topicResponse{
						topic:     t.Topic,
						partition: int(p.Partition),
						index:     index,
						offset:    p.Offset,
						time:      makeTime(p.Timestamp),
						throttle:  makeDuration(res.ThrottleTimeMs),
						err:       makeError(p.ErrorCode, ""),
					}
				}
			}
		}(i, req.Addr, r)
	}

	go func() {
		waitGroup.Wait()
		close(responses)
	}()

	ret := &ListOffsetsResponse{
		Topics: make(map[string][]PartitionOffset),
	}

	results := make(map[topicPartition]PartitionOffset)
	for res := range responses {
		key := topicPartition{res.topic, res.partition}

		partition, ok := results[key]
		if !ok {
			partition = PartitionOffset{
				Partition:   res.partition,
				FirstOffset: -1,
				LastOffset:  -1,
				Offsets:     make(map[int64]time.Time),
			}
		}

		if res.offset >= 0 {
			switch partitionRequests[key][res.index] {
			case FirstOffset:
				partition.FirstOffset = res.offset
			case LastOffset:
				partition.LastOffset = res.offset
			}
			partition.Offsets[res.offset] = res.time
		}

		partition.Error = res.err
		results[key] = partition

		if res.throttle > ret.Throttle {
			ret.Throttle = res.throttle
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	for key, res := range results {
		ret.Topics[key.topic] = append(ret.Topics[key.topic], res)
	}

	for _, partitions := range ret.Topics {
		sort.Slice(partitions, func(i, j int) bool {
			return partitions[i].Partition < partitions[j].Partition
		})
	}

	return ret, nil
}

type listOffsetRequestV1 struct {
	ReplicaID int32
	Topics    []listOffsetRequestTopicV1
}

func (r listOffsetRequestV1) size() int32 {
	return 4 + sizeofArray(len(r.Topics), func(i int) int32 { return r.Topics[i].size() })
}

func (r listOffsetRequestV1) writeTo(wb *writeBuffer) {
	wb.writeInt32(r.ReplicaID)
	wb.writeArray(len(r.Topics), func(i int) { r.Topics[i].writeTo(wb) })
}

type listOffsetRequestTopicV1 struct {
	TopicName  string
	Partitions []listOffsetRequestPartitionV1
}

func (t listOffsetRequestTopicV1) size() int32 {
	return sizeofString(t.TopicName) +
		sizeofArray(len(t.Partitions), func(i int) int32 { return t.Partitions[i].size() })
}

func (t listOffsetRequestTopicV1) writeTo(wb *writeBuffer) {
	wb.writeString(t.TopicName)
	wb.writeArray(len(t.Partitions), func(i int) { t.Partitions[i].writeTo(wb) })
}

type listOffsetRequestPartitionV1 struct {
	Partition int32
	Time      int64
}

func (p listOffsetRequestPartitionV1) size() int32 {
	return 4 + 8
}

func (p listOffsetRequestPartitionV1) writeTo(wb *writeBuffer) {
	wb.writeInt32(p.Partition)
	wb.writeInt64(p.Time)
}

type listOffsetResponseV1 []listOffsetResponseTopicV1

func (r listOffsetResponseV1) size() int32 {
	return sizeofArray(len(r), func(i int) int32 { return r[i].size() })
}

func (r listOffsetResponseV1) writeTo(wb *writeBuffer) {
	wb.writeArray(len(r), func(i int) { r[i].writeTo(wb) })
}

type listOffsetResponseTopicV1 struct {
	TopicName        string
	PartitionOffsets []partitionOffsetV1
}

func (t listOffsetResponseTopicV1) size() int32 {
	return sizeofString(t.TopicName) +
		sizeofArray(len(t.PartitionOffsets), func(i int) int32 { return t.PartitionOffsets[i].size() })
}

func (t listOffsetResponseTopicV1) writeTo(wb *writeBuffer) {
	wb.writeString(t.TopicName)
	wb.writeArray(len(t.PartitionOffsets), func(i int) { t.PartitionOffsets[i].writeTo(wb) })
}

type partitionOffsetV1 struct {
	Partition int32
	ErrorCode int16
	Timestamp int64
	Offset    int64
}

func (p partitionOffsetV1) size() int32 {
	return 4 + 2 + 8 + 8
}

func (p partitionOffsetV1) writeTo(wb *writeBuffer) {
	wb.writeInt32(p.Partition)
	wb.writeInt16(p.ErrorCode)
	wb.writeInt64(p.Timestamp)
	wb.writeInt64(p.Offset)
}

func (p *partitionOffsetV1) readFrom(r *bufio.Reader, sz int) (remain int, err error) {
	if remain, err = readInt32(r, sz, &p.Partition); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &p.ErrorCode); err != nil {
		return
	}
	if remain, err = readInt64(r, remain, &p.Timestamp); err != nil {
		return
	}
	if remain, err = readInt64(r, remain, &p.Offset); err != nil {
		return
	}
	return
}
