package offsetcommit_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/offsetcommit"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

func TestOffsetCommitRequest(t *testing.T) {
	for _, version := range []int16{0} {
		prototest.TestRequest(t, version, &offsetcommit.Request{
			GroupID: "group-0",
			Topics: []offsetcommit.RequestTopic{
				{
					Name: "topic-0",
					Partitions: []offsetcommit.RequestPartition{
						{
							PartitionIndex:    0,
							CommittedOffset:   1,
							CommittedMetadata: "meta-0-0",
						},
						{
							PartitionIndex:    1,
							CommittedOffset:   2,
							CommittedMetadata: "meta-0-1",
						},
					},
				},
			},
		})
	}

	// Version 1 added:
	// GenerationID
	// MemberID
	// RequestTopic.RequestPartition.CommitTimestamp
	for _, version := range []int16{1} {
		prototest.TestRequest(t, version, &offsetcommit.Request{
			GroupID:      "group-1",
			GenerationID: 1,
			MemberID:     "member-1",
			Topics: []offsetcommit.RequestTopic{
				{
					Name: "topic-1",
					Partitions: []offsetcommit.RequestPartition{
						{
							PartitionIndex:    0,
							CommittedOffset:   1,
							CommittedMetadata: "meta-1-0",
							CommitTimestamp:   10,
						},
						{
							PartitionIndex:    1,
							CommittedOffset:   2,
							CommittedMetadata: "meta-1-1",
							CommitTimestamp:   11,
						},
					},
				},
			},
		})
	}

	// Version 2 added:
	// RetentionTimeMs
	// Version 2 removed:
	// RequestTopic.RequestPartition.CommitTimestamp
	// Fields are the same through version 4.
	for _, version := range []int16{2, 3, 4} {
		prototest.TestRequest(t, version, &offsetcommit.Request{
			GroupID:         "group-2",
			GenerationID:    1,
			MemberID:        "member-2",
			RetentionTimeMs: 1999,
			Topics: []offsetcommit.RequestTopic{
				{
					Name: "topic-2",
					Partitions: []offsetcommit.RequestPartition{
						{
							PartitionIndex:    0,
							CommittedOffset:   1,
							CommittedMetadata: "meta-2-0",
						},
						{
							PartitionIndex:    1,
							CommittedOffset:   2,
							CommittedMetadata: "meta-2-1",
						},
					},
				},
			},
		})
	}

	// Version 5 added:
	// RequestTopic.RequestPartition.CommitedLeaderEpoc
	// Version 5 removed:
	// RetentionTimeMs
	// Fields are the same through version 6.
	for _, version := range []int16{5, 6} {
		prototest.TestRequest(t, version, &offsetcommit.Request{
			GroupID:      "group-3",
			GenerationID: 1,
			MemberID:     "member-3",
			Topics: []offsetcommit.RequestTopic{
				{
					Name: "topic-3",
					Partitions: []offsetcommit.RequestPartition{
						{
							PartitionIndex:       0,
							CommittedOffset:      1,
							CommittedMetadata:    "meta-3-0",
							CommittedLeaderEpoch: 10,
						},
						{
							PartitionIndex:       1,
							CommittedOffset:      2,
							CommittedMetadata:    "meta-3-1",
							CommittedLeaderEpoch: 11,
						},
					},
				},
			},
		})
	}

	// Version 7 added:
	// GroupInstanceID
	for _, version := range []int16{7} {
		prototest.TestRequest(t, version, &offsetcommit.Request{
			GroupID:         "group-4",
			GenerationID:    1,
			MemberID:        "member-4",
			GroupInstanceID: "instance-4",
			Topics: []offsetcommit.RequestTopic{
				{
					Name: "topic-4",
					Partitions: []offsetcommit.RequestPartition{
						{
							PartitionIndex:       0,
							CommittedOffset:      1,
							CommittedMetadata:    "meta-4-0",
							CommittedLeaderEpoch: 10,
						},
						{
							PartitionIndex:       1,
							CommittedOffset:      2,
							CommittedMetadata:    "meta-4-1",
							CommittedLeaderEpoch: 11,
						},
					},
				},
			},
		})
	}
}

func TestOffsetCommitResponse(t *testing.T) {
	// Fields are the same through version 2.
	for _, version := range []int16{0, 1, 2} {
		prototest.TestResponse(t, version, &offsetcommit.Response{
			Topics: []offsetcommit.ResponseTopic{
				{
					Name: "topic-1",
					Partitions: []offsetcommit.ResponsePartition{
						{
							PartitionIndex: 4,
							ErrorCode:      34,
						},
					},
				},
			},
		})
	}

	// Version 3 added:
	// ThrottleTimeMs
	// Field are the same through version 7.
	for _, version := range []int16{3, 4, 5, 6, 7} {
		prototest.TestResponse(t, version, &offsetcommit.Response{
			ThrottleTimeMs: 10000,
			Topics: []offsetcommit.ResponseTopic{
				{
					Name: "topic-2",
					Partitions: []offsetcommit.ResponsePartition{
						{
							PartitionIndex: 2,
							ErrorCode:      3,
						},
					},
				},
			},
		})
	}
}
