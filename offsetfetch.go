package kafka

type offsetFetchRequestV1Topic struct {
	// Topic name
	Topic string

	// Partitions to fetch offsets
	Partitions []int32
}

func (t offsetFetchRequestV1Topic) size() int32 {
	return sizeofString(t.Topic) +
		sizeofInt32Array(t.Partitions)
}

func (t offsetFetchRequestV1Topic) writeTo(wb *writeBuffer) {
	wb.writeString(t.Topic)
	wb.writeInt32Array(t.Partitions)
}

type offsetFetchRequestV1 struct {
	// GroupID holds the unique group identifier
	GroupID string

	// Topics to fetch offsets.
	Topics []offsetFetchRequestV1Topic
}

func (t offsetFetchRequestV1) size() int32 {
	return sizeofString(t.GroupID) +
		sizeofArray(len(t.Topics), func(i int) int32 { return t.Topics[i].size() })
}

func (t offsetFetchRequestV1) writeTo(wb *writeBuffer) {
	wb.writeString(t.GroupID)
	wb.writeArray(len(t.Topics), func(i int) { t.Topics[i].writeTo(wb) })
}

type offsetFetchResponseV1PartitionResponse struct {
	// Partition ID
	Partition int32

	// Offset of last committed message
	Offset int64

	// Metadata client wants to keep
	Metadata string

	// ErrorCode holds response error code
	ErrorCode int16
}

func (t offsetFetchResponseV1PartitionResponse) size() int32 {
	return sizeofInt32(t.Partition) +
		sizeofInt64(t.Offset) +
		sizeofString(t.Metadata) +
		sizeofInt16(t.ErrorCode)
}

func (t offsetFetchResponseV1PartitionResponse) writeTo(wb *writeBuffer) {
	wb.writeInt32(t.Partition)
	wb.writeInt64(t.Offset)
	wb.writeString(t.Metadata)
	wb.writeInt16(t.ErrorCode)
}

func (t *offsetFetchResponseV1PartitionResponse) readFrom(rb *readBuffer) {
	t.Partition = rb.readInt32()
	t.Offset = rb.readInt64()
	t.Metadata = rb.readString()
	t.ErrorCode = rb.readInt16()
}

type offsetFetchResponseV1Response struct {
	// Topic name
	Topic string

	// PartitionResponses holds offsets by partition
	PartitionResponses []offsetFetchResponseV1PartitionResponse
}

func (t offsetFetchResponseV1Response) size() int32 {
	return sizeofString(t.Topic) +
		sizeofArray(len(t.PartitionResponses), func(i int) int32 { return t.PartitionResponses[i].size() })
}

func (t offsetFetchResponseV1Response) writeTo(wb *writeBuffer) {
	wb.writeString(t.Topic)
	wb.writeArray(len(t.PartitionResponses), func(i int) { t.PartitionResponses[i].writeTo(wb) })
}

func (t *offsetFetchResponseV1Response) readFrom(rb *readBuffer) {
	t.Topic = rb.readString()

	rb.readArray(func() {
		res := offsetFetchResponseV1PartitionResponse{}
		res.readFrom(rb)
		t.PartitionResponses = append(t.PartitionResponses, res)
	})
}

type offsetFetchResponseV1 struct {
	// Responses holds topic partition offsets
	Responses []offsetFetchResponseV1Response
}

func (t offsetFetchResponseV1) size() int32 {
	return sizeofArray(len(t.Responses), func(i int) int32 { return t.Responses[i].size() })
}

func (t offsetFetchResponseV1) writeTo(wb *writeBuffer) {
	wb.writeArray(len(t.Responses), func(i int) { t.Responses[i].writeTo(wb) })
}

func (t *offsetFetchResponseV1) readFrom(rb *readBuffer) {
	rb.readArray(func() {
		res := offsetFetchResponseV1Response{}
		res.readFrom(rb)
		t.Responses = append(t.Responses, res)
	})
}

func findOffset(topic string, partition int32, response offsetFetchResponseV1) (int64, bool) {
	for _, r := range response.Responses {
		if r.Topic != topic {
			continue
		}

		for _, pr := range r.PartitionResponses {
			if pr.Partition == partition {
				return pr.Offset, true
			}
		}
	}

	return 0, false
}
