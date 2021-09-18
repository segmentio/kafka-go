package txnoffsetcommit_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/prototest"
	"github.com/segmentio/kafka-go/protocol/txnoffsetcommit"
)

func TestTxnOffsetCommitRequest(t *testing.T) {
	for _, version := range []int16{0, 1} {
		prototest.TestRequest(t, version, &txnoffsetcommit.Request{
			TransactionalID: "transactional-id-0",
			GroupID:         "group-0",
			ProducerID:      10,
			ProducerEpoch:   100,
			Topics: []txnoffsetcommit.RequestTopic{
				{
					Name: "topic-0",
					Partitions: []txnoffsetcommit.RequestPartition{
						{
							Partition:         0,
							CommittedOffset:   10,
							CommittedMetadata: "meta-0-0",
						},
						{
							Partition:         1,
							CommittedOffset:   10,
							CommittedMetadata: "meta-0-1",
						},
					},
				},
				{
					Name: "topic-1",
					Partitions: []txnoffsetcommit.RequestPartition{
						{
							Partition:         0,
							CommittedOffset:   10,
							CommittedMetadata: "meta-1-0",
						},
						{
							Partition:         1,
							CommittedOffset:   10,
							CommittedMetadata: "meta-1-1",
						},
					},
				},
			},
		})
	}

	// Version 2 added:
	// Topics.RequestTopic.Partitions.CommittedLeaderEpoch
	for _, version := range []int16{2} {
		prototest.TestRequest(t, version, &txnoffsetcommit.Request{
			TransactionalID: "transactional-id-0",
			GroupID:         "group-0",
			ProducerID:      10,
			ProducerEpoch:   100,
			Topics: []txnoffsetcommit.RequestTopic{
				{
					Name: "topic-0",
					Partitions: []txnoffsetcommit.RequestPartition{
						{
							Partition:            0,
							CommittedOffset:      10,
							CommittedLeaderEpoch: 100,
							CommittedMetadata:    "meta-0-0",
						},
						{
							Partition:            1,
							CommittedOffset:      10,
							CommittedLeaderEpoch: 100,
							CommittedMetadata:    "meta-0-1",
						},
					},
				},
				{
					Name: "topic-1",
					Partitions: []txnoffsetcommit.RequestPartition{
						{
							Partition:            0,
							CommittedOffset:      10,
							CommittedLeaderEpoch: 100,
							CommittedMetadata:    "meta-1-0",
						},
						{
							Partition:            1,
							CommittedOffset:      10,
							CommittedLeaderEpoch: 100,
							CommittedMetadata:    "meta-1-1",
						},
					},
				},
			},
		})
	}

	// Version 3 added:
	// GenerationID
	// MemberID
	// GroupInstanceID
	for _, version := range []int16{3} {
		prototest.TestRequest(t, version, &txnoffsetcommit.Request{
			TransactionalID: "transactional-id-0",
			GroupID:         "group-0",
			ProducerID:      10,
			ProducerEpoch:   100,
			GenerationID:    2,
			MemberID:        "member-0",
			GroupInstanceID: "group-instance-id-0",
			Topics: []txnoffsetcommit.RequestTopic{
				{
					Name: "topic-0",
					Partitions: []txnoffsetcommit.RequestPartition{
						{
							Partition:            0,
							CommittedOffset:      10,
							CommittedLeaderEpoch: 100,
							CommittedMetadata:    "meta-0-0",
						},
						{
							Partition:            1,
							CommittedOffset:      10,
							CommittedLeaderEpoch: 100,
							CommittedMetadata:    "meta-0-1",
						},
					},
				},
				{
					Name: "topic-1",
					Partitions: []txnoffsetcommit.RequestPartition{
						{
							Partition:            0,
							CommittedOffset:      10,
							CommittedLeaderEpoch: 100,
							CommittedMetadata:    "meta-1-0",
						},
						{
							Partition:            1,
							CommittedOffset:      10,
							CommittedLeaderEpoch: 100,
							CommittedMetadata:    "meta-1-1",
						},
					},
				},
			},
		})
	}
}

func TestTxnOffsetCommitResponse(t *testing.T) {
	for _, version := range []int16{0, 1, 2, 3} {
		prototest.TestResponse(t, version, &txnoffsetcommit.Response{
			ThrottleTimeMs: 10,
			Topics: []txnoffsetcommit.ResponseTopic{
				{
					Name: "topic-0",
					Partitions: []txnoffsetcommit.ResponsePartition{
						{
							Partition: 0,
							ErrorCode: 0,
						},
						{
							Partition: 1,
							ErrorCode: 10,
						},
					},
				},
				{
					Name: "topic-1",
					Partitions: []txnoffsetcommit.ResponsePartition{
						{
							Partition: 0,
							ErrorCode: 0,
						},
						{
							Partition: 1,
							ErrorCode: 10,
						},
					},
				},
			},
		})
	}
}
