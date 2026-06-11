package kafka

import (
	"io"
	"testing"

	fetchAPI "github.com/segmentio/kafka-go/protocol/fetch"
	meta "github.com/segmentio/kafka-go/protocol/metadata"
	produceAPI "github.com/segmentio/kafka-go/protocol/produce"
)

func TestIsStaleMetadataError(t *testing.T) {
	staleErrors := []Error{
		UnknownTopicOrPartition,
		LeaderNotAvailable,
		NotLeaderForPartition,
		ReplicaNotAvailable,
		BrokerNotAvailable,
		KafkaStorageError,
		FencedLeaderEpoch,
		UnknownLeaderEpoch,
		UnknownTopicID,
	}

	for _, err := range staleErrors {
		if !isStaleMetadataError(err) {
			t.Errorf("expected %v (%d) to be classified as a stale metadata error", err, int(err))
		}
	}

	nonStaleErrors := []Error{
		Unknown,
		InvalidMessage,
		RequestTimedOut,
		MessageSizeTooLarge,
		TopicAuthorizationFailed,
		InvalidRequiredAcks,
	}

	for _, err := range nonStaleErrors {
		if isStaleMetadataError(err) {
			t.Errorf("did not expect %v (%d) to be classified as a stale metadata error", err, int(err))
		}
	}
}

func TestErrorRequiresMetadataRefresh(t *testing.T) {
	tests := []struct {
		scenario string
		err      error
		want     bool
	}{
		{scenario: "nil", err: nil, want: false},
		{scenario: "transient network", err: io.ErrUnexpectedEOF, want: true},
		{scenario: "broker not available", err: BrokerNotAvailable, want: true},
		{scenario: "not leader for partition", err: NotLeaderForPartition, want: true},
		{scenario: "non retriable kafka error", err: TopicAuthorizationFailed, want: false},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			if got := errorRequiresMetadataRefresh(test.err); got != test.want {
				t.Errorf("errorRequiresMetadataRefresh(%v) = %v, want %v", test.err, got, test.want)
			}
		})
	}
}

func TestResponseRequiresMetadataRefresh(t *testing.T) {
	produceWith := func(code Error) *produceAPI.Response {
		return &produceAPI.Response{
			Topics: []produceAPI.ResponseTopic{{
				Topic: "topic",
				Partitions: []produceAPI.ResponsePartition{{
					Partition: 0,
					ErrorCode: int16(code),
				}},
			}},
		}
	}

	fetchWith := func(topLevel, partition Error) *fetchAPI.Response {
		return &fetchAPI.Response{
			ErrorCode: int16(topLevel),
			Topics: []fetchAPI.ResponseTopic{{
				Topic: "topic",
				Partitions: []fetchAPI.ResponsePartition{{
					Partition: 0,
					ErrorCode: int16(partition),
				}},
			}},
		}
	}

	tests := []struct {
		scenario string
		resp     Response
		want     bool
	}{
		{scenario: "produce stale", resp: produceWith(NotLeaderForPartition), want: true},
		{scenario: "produce ok", resp: produceWith(0), want: false},
		{scenario: "produce non-stale error", resp: produceWith(InvalidMessageSize), want: false},
		{scenario: "fetch partition stale", resp: fetchWith(0, LeaderNotAvailable), want: true},
		{scenario: "fetch session stale", resp: fetchWith(FencedLeaderEpoch, 0), want: true},
		{scenario: "fetch ok", resp: fetchWith(0, 0), want: false},
		{scenario: "unrelated response", resp: &meta.Response{}, want: false},
		{scenario: "nil interface", resp: nil, want: false},
		{scenario: "typed nil produce", resp: (*produceAPI.Response)(nil), want: false},
		{scenario: "typed nil fetch", resp: (*fetchAPI.Response)(nil), want: false},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			if got := responseRequiresMetadataRefresh(test.resp); got != test.want {
				t.Errorf("responseRequiresMetadataRefresh(%T) = %v, want %v", test.resp, got, test.want)
			}
		})
	}
}
