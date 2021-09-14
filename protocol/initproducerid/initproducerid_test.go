package initproducerid_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/initproducerid"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

func TestInitProducerIDRequest(t *testing.T) {
	for _, version := range []int16{0, 1, 2} {
		prototest.TestRequest(t, version, &initproducerid.Request{
			TransactionalID:      "transactional-id-0",
			TransactionTimeoutMs: 1000,
		})
	}

	// Version 2 added:
	// ProducerID
	// ProducerEpoch
	for _, version := range []int16{3, 4} {
		prototest.TestRequest(t, version, &initproducerid.Request{
			TransactionalID:      "transactional-id-0",
			TransactionTimeoutMs: 1000,
			ProducerID:           10,
			ProducerEpoch:        5,
		})
	}
}

func TestInitProducerIDResponse(t *testing.T) {
	for _, version := range []int16{0, 1, 2, 3, 4} {
		prototest.TestResponse(t, version, &initproducerid.Response{
			ThrottleTimeMs: 1000,
			ErrorCode:      9,
			ProducerID:     10,
			ProducerEpoch:  1000,
		})
	}
}
