package kafka

import "fmt"

// Error represents the different error codes that may be returned by kafka.
type Error int

const (
	NoError                      Error = 0
	Unknown                      Error = -1
	OffsetOutOfRange             Error = 1
	InvalidMessage               Error = 2
	UnknownTopicOrPartition      Error = 3
	InvalidMessageSize           Error = 4
	LeaderNotAvailable           Error = 5
	NotLeaderForPartition        Error = 6
	RequestTimedOut              Error = 7
	BrokerNotAvailable           Error = 8
	ReplicaNotAvailable          Error = 9
	MessageSizeTooLarge          Error = 10
	StaleControllerEpoch         Error = 11
	OffsetMetadataTooLarge       Error = 12
	GroupLoadInProgress          Error = 14
	GroupCoordinatorNotAvailable Error = 15
	NotCoordinatorForGroup       Error = 16
	InvalidTopic                 Error = 17
	RecordListTooLarge           Error = 18
	NotEnoughReplicas            Error = 19
	NotEnoughReplicasAfterAppend Error = 20
	InvalidRequiredAcks          Error = 21
	IllegalGeneration            Error = 22
	InconsistentGroupProtocol    Error = 23
	InvalidGroupId               Error = 24
	UnknownMemberId              Error = 25
	InvalidSessionTimeout        Error = 26
	RebalanceInProgress          Error = 27
	InvalidCommitOffsetSize      Error = 28
	TopicAuthorizationFailed     Error = 29
	GroupAuthorizationFailed     Error = 30
	ClusterAuthorizationFailed   Error = 31
)

// Error satisfies the error interface.
func (e Error) Error() string {
	return fmt.Sprintf("%d %s: %s", e, e.Title(), e.Description())
}

// Timeout returns true if the error was due to a timeout.
func (e Error) Timeout() bool {
	return e == RequestTimedOut
}

// Temporary returns true if the operation that generated the error may succeed
// if retried at a later time.
func (e Error) Temporary() bool {
	return e == LeaderNotAvailable ||
		e == BrokerNotAvailable ||
		e == ReplicaNotAvailable ||
		e == GroupLoadInProgress ||
		e == GroupCoordinatorNotAvailable ||
		e == RebalanceInProgress ||
		e.Timeout()
}

// Title returns a human readable title for the error.
func (e Error) Title() string {
	switch e {
	case NoError:
		return "no error"
	case Unknown:
		return "unknown"
	case OffsetOutOfRange:
		return "ouffset out of range"
	case InvalidMessage:
		return "invalid message"
	case UnknownTopicOrPartition:
		return "unknown topic or partition"
	case InvalidMessageSize:
		return "invalid message size"
	case LeaderNotAvailable:
		return "leader not available"
	case NotLeaderForPartition:
		return "not leader for patition"
	case RequestTimedOut:
		return "request timed out"
	case BrokerNotAvailable:
		return "broker not available"
	case ReplicaNotAvailable:
		return "replica not available"
	case MessageSizeTooLarge:
		return "message size too large"
	case StaleControllerEpoch:
		return "stale controller epoch"
	case OffsetMetadataTooLarge:
		return "offset metadata too large"
	case GroupLoadInProgress:
		return "group load in progress"
	case GroupCoordinatorNotAvailable:
		return "group corrdinator not available"
	case NotCoordinatorForGroup:
		return "not coordinator for group"
	case InvalidTopic:
		return "invalid topic"
	case RecordListTooLarge:
		return "record list too large"
	case NotEnoughReplicas:
		return "not enough replicas"
	case NotEnoughReplicasAfterAppend:
		return "not enough replicas after append"
	case InvalidRequiredAcks:
		return "invalid required acks"
	case IllegalGeneration:
		return "illegal generation"
	case InconsistentGroupProtocol:
		return "inconsistent group protocol"
	case InvalidGroupId:
		return "invalid group id"
	case UnknownMemberId:
		return "unknown member id"
	case InvalidSessionTimeout:
		return "invalid session timeout"
	case RebalanceInProgress:
		return "rebalance in progress"
	case InvalidCommitOffsetSize:
		return "invalid commit offset size"
	case TopicAuthorizationFailed:
		return "topic authorization failed"
	case GroupAuthorizationFailed:
		return "group authorization failed"
	case ClusterAuthorizationFailed:
		return "cluster authorization failed"
	}
	return ""
}

// Description returns a human readable description of cause of the error.
func (e Error) Description() string {
	switch e {
	case NoError:
		return "it worked!"
	case Unknown:
		return "an unexpected server error occurred"
	case OffsetOutOfRange:
		return "the requested offset is outside the range of offsets maintained by the server for the given topic/partition"
	case InvalidMessage:
		return "the message contents does not match its CRC"
	case UnknownTopicOrPartition:
		return "the request is for a topic or partition that does not exist on this broker"
	case InvalidMessageSize:
		return "the message has a negative size"
	case LeaderNotAvailable:
		return "the cluster is in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes"
	case NotLeaderForPartition:
		return "the client attempted to send messages to a replica that is not the leader for some partition, the client's metadata are likely out of date"
	case RequestTimedOut:
		return "the request exceeded the user-specified time limit in the request"
	case BrokerNotAvailable:
		return "not a client facing error and is used mostly by tools when a broker is not alive"
	case ReplicaNotAvailable:
		return "a replica is expected on a broker, but is not (this can be safely ignored)"
	case MessageSizeTooLarge:
		return "the server has a configurable maximum message size to avoid unbounded memory allocation and the client attempted to produce a message larger than this maximum"
	case StaleControllerEpoch:
		return "internal error code for broker-to-broker communication"
	case OffsetMetadataTooLarge:
		return "the client specified a string larger than configured maximum for offset metadata"
	case GroupLoadInProgress:
		return "the broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition), or in response to group membership requests (such as heartbeats) when group metadata is being loaded by the coordinator"
	case GroupCoordinatorNotAvailable:
		return "the broker returns this error code for group coordinator requests, offset commits, and most group management requests if the offsets topic has not yet been created, or if the group coordinator is not active"
	case NotCoordinatorForGroup:
		return "the broker returns this error code if it receives an offset fetch or commit request for a group that it is not a coordinator for"
	case InvalidTopic:
		return "a request which attempted to access an invalid topic (e.g. one which has an illegal name), or if an attempt was made to write to an internal topic (such as the consumer offsets topic)"
	case RecordListTooLarge:
		return "a message batch in a produce request exceeds the maximum configured segment size"
	case NotEnoughReplicas:
		return "the number of in-sync replicas is lower than the configured minimum and requiredAcks is -1"
	case NotEnoughReplicasAfterAppend:
		return "the message was written to the log, but with fewer in-sync replicas than required."
	case InvalidRequiredAcks:
		return "the requested requiredAcks is invalid (anything other than -1, 1, or 0)"
	case IllegalGeneration:
		return "the generation id provided in the request is not the current generation"
	case InconsistentGroupProtocol:
		return "the member provided a protocol type or set of protocols which is not compatible with the current group"
	case InvalidGroupId:
		return "the group id is empty or null"
	case UnknownMemberId:
		return "the member id is not in the current generation"
	case InvalidSessionTimeout:
		return "the requested session timeout is outside of the allowed range on the broker"
	case RebalanceInProgress:
		return "the coordinator has begun rebalancing the group, the client should rejoin the group"
	case InvalidCommitOffsetSize:
		return "an offset commit was rejected because of oversize metadata"
	case TopicAuthorizationFailed:
		return "the client is not authorized to access the requested topic"
	case GroupAuthorizationFailed:
		return "the client is not authorized to access a particular group id"
	case ClusterAuthorizationFailed:
		return "the client is not authorized to use an inter-broker or administrative API"
	}
	return ""
}

func isTimeout(err error) bool {
	e, ok := err.(interface {
		Timeout() bool
	})
	return ok && e.Timeout()
}

func isTemporary(err error) bool {
	e, ok := err.(interface {
		Temporary() bool
	})
	return ok && e.Temporary()
}
