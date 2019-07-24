package kafka

import (
	"testing"
)

func TestOffsetFetchResponseV1(t *testing.T) {
	testProtocolType(t,
		&offsetFetchResponseV1{
			Responses: []offsetFetchResponseV1Response{{
				Topic: "a",
				PartitionResponses: []offsetFetchResponseV1PartitionResponse{{
					Partition: 2,
					Offset:    3,
					Metadata:  "b",
					ErrorCode: 4,
				}},
			}},
		},
		&offsetFetchResponseV1{},
	)
}
