package kafka

import (
	"fmt"
	"testing"
)

func TestError(t *testing.T) {
	t.Parallel()

	errorCodes := []Error{
		Unknown,
		OffsetOutOfRange,
		InvalidMessage,
		UnknownTopicOrPartition,
		InvalidMessageSize,
		LeaderNotAvailable,
		NotLeaderForPartition,
		RequestTimedOut,
		BrokerNotAvailable,
		ReplicaNotAvailable,
		MessageSizeTooLarge,
		StaleControllerEpoch,
		OffsetMetadataTooLarge,
		GroupLoadInProgress,
		GroupCoordinatorNotAvailable,
		NotCoordinatorForGroup,
		InvalidTopic,
		RecordListTooLarge,
		NotEnoughReplicas,
		NotEnoughReplicasAfterAppend,
		InvalidRequiredAcks,
		IllegalGeneration,
		InconsistentGroupProtocol,
		InvalidGroupId,
		UnknownMemberId,
		InvalidSessionTimeout,
		RebalanceInProgress,
		InvalidCommitOffsetSize,
		TopicAuthorizationFailed,
		GroupAuthorizationFailed,
		ClusterAuthorizationFailed,
		InvalidTimestamp,
		UnsupportedSASLMechanism,
		IllegalSASLState,
		UnsupportedVersion,
		TopicAlreadyExists,
		InvalidPartitionNumber,
		InvalidReplicationFactor,
		InvalidReplicaAssignment,
		InvalidConfiguration,
		NotController,
		InvalidRequest,
		UnsupportedForMessageFormat,
		PolicyViolation,
		OutOfOrderSequenceNumber,
		DuplicateSequenceNumber,
		InvalidProducerEpoch,
		InvalidTransactionState,
		InvalidProducerIDMapping,
		InvalidTransactionTimeout,
		ConcurrentTransactions,
		TransactionCoordinatorFenced,
		TransactionalIDAuthorizationFailed,
		SecurityDisabled,
		BrokerAuthorizationFailed,
		KafkaStorageError,
		LogDirNotFound,
		SASLAuthenticationFailed,
		UnknownProducerId,
		ReassignmentInProgress,
		DelegationTokenAuthDisabled,
		DelegationTokenNotFound,
		DelegationTokenOwnerMismatch,
		DelegationTokenRequestNotAllowed,
		DelegationTokenAuthorizationFailed,
		DelegationTokenExpired,
		InvalidPrincipalType,
		NonEmptyGroup,
		GroupIdNotFound,
		FetchSessionIDNotFound,
		InvalidFetchSessionEpoch,
		ListenerNotFound,
		TopicDeletionDisabled,
		FencedLeaderEpoch,
		UnknownLeaderEpoch,
		UnsupportedCompressionType,
	}

	for _, err := range errorCodes {
		t.Run(fmt.Sprintf("verify that error %d has a non-empty title, description, and error message", err), func(t *testing.T) {
			if len(err.Title()) == 0 {
				t.Error("empty title")
			}
			if len(err.Description()) == 0 {
				t.Error("empty description")
			}
			if len(err.Error()) == 0 {
				t.Error("empty error message")
			}
		})
	}

	t.Run("verify that an invalid error code has an empty title and description", func(t *testing.T) {
		err := Error(-2)

		if s := err.Title(); len(s) != 0 {
			t.Error("non-empty title:", s)
		}

		if s := err.Description(); len(s) != 0 {
			t.Error("non-empty description:", s)
		}
	})
}
