package endtxn_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/endtxn"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

func TestEndTxnRequest(t *testing.T) {
	for _, version := range []int16{0, 1, 2, 3, 4, 5} {
		prototest.TestRequest(t, version, &endtxn.Request{
			TransactionalID: "transactional-id-1",
			ProducerID:      1,
			ProducerEpoch:   100,
			Committed:       false,
		})
	}
}

func TestEndTxnResponse(t *testing.T) {
	for _, version := range []int16{0, 1, 2, 3, 4} {
		prototest.TestResponse(t, version, &endtxn.Response{
			ThrottleTimeMs: 1000,
			ErrorCode:      4,
		})
	}

	prototest.TestResponse(t, 5, &endtxn.Response{
		ThrottleTimeMs: 1000,
		ErrorCode:      0,
		ProducerID:     12345,
		ProducerEpoch:  101,
	})
}
