package kafka

import (
	"errors"

	"github.com/segmentio/kafka-go/protocol"
	fetchAPI "github.com/segmentio/kafka-go/protocol/fetch"
	produceAPI "github.com/segmentio/kafka-go/protocol/produce"
)

// isStaleMetadataError reports whether a kafka error code indicates that the
// cached cluster metadata is likely out of date and should be refreshed.
func isStaleMetadataError(err Error) bool {
	switch err {
	case UnknownTopicOrPartition,
		LeaderNotAvailable,
		NotLeaderForPartition,
		ReplicaNotAvailable,
		BrokerNotAvailable,
		KafkaStorageError,
		FencedLeaderEpoch,
		UnknownLeaderEpoch,
		UnknownTopicID:
		return true
	default:
		return false
	}
}

// errorRequiresMetadataRefresh reports whether a transport-level error returned
// by a failed round trip likely indicates stale cluster metadata: transient
// network errors, routing failures over a stale layout (unknown topic, partition
// or leader), or kafka errors that signal the metadata is out of date.
func errorRequiresMetadataRefresh(err error) bool {
	if isTransientNetworkError(err) {
		return true
	}
	// Routing failures raised while resolving the target broker from the cached
	// cluster layout indicate the layout is stale and should be refreshed.
	if errors.Is(err, protocol.ErrNoTopic) ||
		errors.Is(err, protocol.ErrNoPartition) ||
		errors.Is(err, protocol.ErrNoLeader) {
		return true
	}
	var kafkaErr Error
	if errors.As(err, &kafkaErr) {
		return isStaleMetadataError(kafkaErr)
	}
	return false
}

// responseRequiresMetadataRefresh inspects a successful response body and
// reports whether any partition (or, for Fetch, the session-level error code)
// reported a stale-metadata error. Only response types routed to specific
// partition leaders are inspected, since those are impacted by leader changes.
func responseRequiresMetadataRefresh(r Response) bool {
	switch resp := r.(type) {
	case *produceAPI.Response:
		if resp == nil {
			return false
		}
		for i := range resp.Topics {
			partitions := resp.Topics[i].Partitions
			for j := range partitions {
				if isStaleMetadataError(Error(partitions[j].ErrorCode)) {
					return true
				}
			}
		}
	case *fetchAPI.Response:
		if resp == nil {
			return false
		}
		// Fetch v7+ may report a session-level error in addition to the
		// per-partition error codes.
		if isStaleMetadataError(Error(resp.ErrorCode)) {
			return true
		}
		for i := range resp.Topics {
			partitions := resp.Topics[i].Partitions
			for j := range partitions {
				if isStaleMetadataError(Error(partitions[j].ErrorCode)) {
					return true
				}
			}
		}
	}
	return false
}
