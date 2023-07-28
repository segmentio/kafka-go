package kafka

import (
	"context"
	"errors"
	"testing"

	ktesting "github.com/segmentio/kafka-go/testing"
	"github.com/stretchr/testify/assert"
)

func TestDescribeUserScramCredentials(t *testing.T) {
	// https://issues.apache.org/jira/browse/KAFKA-10259
	if !ktesting.KafkaIsAtLeast("2.7.0") {
		return
	}

	client, shutdown := newLocalClient()
	defer shutdown()

	name := makeTopic()

	createRes, err := client.AlterUserScramCredentials(context.Background(), &AlterUserScramCredentialsRequest{
		Upsertions: []UserScramCredentialsUpsertion{
			{
				Name:           name,
				Mechanism:      ScramMechanismSha512,
				Iterations:     15000,
				Salt:           []byte("my-salt"),
				SaltedPassword: []byte("my-salted-password"),
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(createRes.Results) != 1 {
		t.Fatalf("expected 1 createResult; got %d", len(createRes.Results))
	}

	if createRes.Results[0].User != name {
		t.Fatalf("expected createResult with user: %s, got %s", name, createRes.Results[0].User)
	}

	if createRes.Results[0].Error != nil {
		t.Fatalf("didn't expect an error in createResult, got %v", createRes.Results[0].Error)
	}

	describeCreationRes, err := client.DescribeUserScramCredentials(context.Background(), &DescribeUserScramCredentialsRequest{
		Users: []UserScramCredentialsUser{
			{
				Name: name,
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	expectedCreation := DescribeUserScramCredentialsResponse{
		Throttle: makeDuration(0),
		Error:    makeError(0, ""),
		Results: []DescribeUserScramCredentialsResponseResult{
			{
				User: name,
				CredentialInfos: []DescribeUserScramCredentialsCredentialInfo{
					{
						Mechanism:  ScramMechanismSha512,
						Iterations: 15000,
					},
				},
				Error: makeError(0, ""),
			},
		},
	}

	assert.Equal(t, expectedCreation, *describeCreationRes)

	deleteRes, err := client.AlterUserScramCredentials(context.Background(), &AlterUserScramCredentialsRequest{
		Deletions: []UserScramCredentialsDeletion{
			{
				Name:      name,
				Mechanism: ScramMechanismSha512,
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(deleteRes.Results) != 1 {
		t.Fatalf("expected 1 deleteResult; got %d", len(deleteRes.Results))
	}

	if deleteRes.Results[0].User != name {
		t.Fatalf("expected deleteResult with user: %s, got %s", name, deleteRes.Results[0].User)
	}

	if deleteRes.Results[0].Error != nil {
		t.Fatalf("didn't expect an error in deleteResult, got %v", deleteRes.Results[0].Error)
	}

	describeDeletionRes, err := client.DescribeUserScramCredentials(context.Background(), &DescribeUserScramCredentialsRequest{
		Users: []UserScramCredentialsUser{
			{
				Name: name,
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	if !errors.Is(describeDeletionRes.Error, makeError(0, "")) {
		t.Fatalf("didn't expect a top level error on describe results after deletion, got %v", describeDeletionRes.Error)
	}

	if len(describeDeletionRes.Results) != 1 {
		t.Fatalf("expected one describe results after deletion, got %d describe results", len(describeDeletionRes.Results))
	}

	result := describeDeletionRes.Results[0]

	if result.User != name {
		t.Fatalf("expected describeResult with user: %s, got %s", name, result.User)
	}

	if len(result.CredentialInfos) != 0 {
		t.Fatalf("didn't expect describeResult credential infos, got %v", result.CredentialInfos)
	}

	if !errors.Is(result.Error, ResourceNotFound) {
		t.Fatalf("expected describeResult resourcenotfound error, got %s", result.Error)
	}
}
