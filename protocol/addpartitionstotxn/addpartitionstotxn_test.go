package addpartitionstotxn_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/addpartitionstotxn"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

func TestAddPartitionsToTxnRequest(t *testing.T) {
	for _, version := range []int16{0, 1, 2, 3} {
		prototest.TestRequest(t, version, &addpartitionstotxn.Request{
			TransactionalID: "transaction-id-0",
			ProducerID:      10,
			ProducerEpoch:   100,
			Topics: []addpartitionstotxn.RequestTopic{
				{
					Name:       "topic-1",
					Partitions: []int32{0, 1, 2, 3},
				},
				{
					Name:       "topic-2",
					Partitions: []int32{0, 1, 2},
				},
			},
		})
	}
}

func TestAddPartitionsToTxnResponse(t *testing.T) {
	for _, version := range []int16{0, 1, 2, 3} {
		prototest.TestResponse(t, version, &addpartitionstotxn.Response{
			ThrottleTimeMs: 20,
			Results: []addpartitionstotxn.ResponseResult{
				{
					Name: "topic-1",
					Results: []addpartitionstotxn.ResponsePartition{
						{
							PartitionIndex: 0,
							ErrorCode:      19,
						},
						{
							PartitionIndex: 1,
							ErrorCode:      0,
						},
					},
				},
				{
					Name: "topic-2",
					Results: []addpartitionstotxn.ResponsePartition{
						{
							PartitionIndex: 0,
							ErrorCode:      0,
						},
					},
				},
			},
		})
	}
}
