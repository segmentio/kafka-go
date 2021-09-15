package addoffsetstotxn_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/addoffsetstotxn"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

func TestAddOffsetsToTxnRequest(t *testing.T) {
	for _, version := range []int16{0, 1, 2, 3} {
		prototest.TestRequest(t, version, &addoffsetstotxn.Request{
			TransactionalID: "transactional-id-0",
			ProducerID:      1,
			ProducerEpoch:   10,
			GroupID:         "group-id-0",
		})
	}
}

func TestAddOffsetsToTxnResponse(t *testing.T) {
	for _, version := range []int16{0, 1, 2, 3} {
		prototest.TestResponse(t, version, &addoffsetstotxn.Response{
			ThrottleTimeMs: 10,
			ErrorCode:      1,
		})
	}
}
