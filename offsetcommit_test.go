package kafka

import (
	"testing"
)

func TestOffsetCommitResponseV2(t *testing.T) {
	testProtocolType(t,
		&offsetCommitResponseV2{
			Responses: []offsetCommitResponseV2Response{{
				Topic: "a",
				PartitionResponses: []offsetCommitResponseV2PartitionResponse{{
					Partition: 1,
					ErrorCode: 2,
				}},
			}},
		},
		&offsetCommitResponseV2{},
	)
}
