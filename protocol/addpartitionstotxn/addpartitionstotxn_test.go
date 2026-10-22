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

	for _, version := range []int16{4, 5} {
		prototest.TestRequest(t, version, &addpartitionstotxn.Request{
			Transactions: []addpartitionstotxn.RequestTransaction{
				{
					TransactionalID: "transaction-id-0",
					ProducerID:      10,
					ProducerEpoch:   100,
					VerifyOnly:      true,
					Topics: []addpartitionstotxn.RequestTopic{
						{
							Name:       "topic-1",
							Partitions: []int32{0, 1, 2, 3},
						},
					},
				},
			},
		})
	}
}

func TestAddPartitionsToTxnPrepare(t *testing.T) {
	// Prepare for v4+: converts top-level fields to Transactions array
	req := &addpartitionstotxn.Request{
		TransactionalID: "txn-1",
		ProducerID:      123,
		ProducerEpoch:   45,
		Topics: []addpartitionstotxn.RequestTopic{
			{Name: "topic-1", Partitions: []int32{0}},
		},
	}
	req.Prepare(4)
	if len(req.Transactions) != 1 {
		t.Fatalf("expected 1 transaction in Transactions, got %d", len(req.Transactions))
	}
	if req.Transactions[0].TransactionalID != "txn-1" {
		t.Errorf("expected txn-1, got %s", req.Transactions[0].TransactionalID)
	}
	if req.Transaction() != "txn-1" {
		t.Errorf("expected Transaction() to return txn-1, got %s", req.Transaction())
	}

	// Prepare for v3 and below: converts Transactions array to top-level fields
	v4Req := &addpartitionstotxn.Request{
		Transactions: []addpartitionstotxn.RequestTransaction{
			{
				TransactionalID: "txn-2",
				ProducerID:      456,
				ProducerEpoch:   78,
				Topics: []addpartitionstotxn.RequestTopic{
					{Name: "topic-2", Partitions: []int32{1}},
				},
			},
		},
	}
	v4Req.Prepare(3)
	if v4Req.TransactionalID != "txn-2" {
		t.Errorf("expected txn-2, got %s", v4Req.TransactionalID)
	}
	if v4Req.ProducerID != 456 {
		t.Errorf("expected 456, got %d", v4Req.ProducerID)
	}
	if v4Req.ProducerEpoch != 78 {
		t.Errorf("expected 78, got %d", v4Req.ProducerEpoch)
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

	for _, version := range []int16{4, 5} {
		prototest.TestResponse(t, version, &addpartitionstotxn.Response{
			ThrottleTimeMs: 20,
			ErrorCode:      0,
			ResultsByTransaction: []addpartitionstotxn.ResponseTransactionResult{
				{
					TransactionalID: "transaction-id-0",
					TopicResults: []addpartitionstotxn.ResponseResult{
						{
							Name: "topic-1",
							Results: []addpartitionstotxn.ResponsePartition{
								{
									PartitionIndex: 0,
									ErrorCode:      0,
								},
							},
						},
					},
				},
			},
		})
	}
}
