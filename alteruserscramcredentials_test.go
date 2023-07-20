package kafka

import (
	"context"
	"testing"

	ktesting "github.com/segmentio/kafka-go/testing"
	"github.com/stretchr/testify/assert"
)

func TestAlterUserScramCredentials(t *testing.T) {
	// https://issues.apache.org/jira/browse/KAFKA-10259
	if !ktesting.KafkaIsAtLeast("2.7.0") {
		return
	}

	client, shutdown := newLocalClient()
	defer shutdown()

	createRes, err := client.AlterUserScramCredentials(context.Background(), &AlterUserScramCredentialsRequest{
		Upsertions: []UserScramCredentialsUpsertion{
			{
				Name:           "alice",
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

	for _, err := range createRes.Errors {
		if err != nil {
			t.Error(err)
		}
	}

	if len(createRes.Results) != 1 {
		t.Fatalf("expected 1 createResult; got %d", len(createRes.Results))
	}

	if createRes.Results[0].User != "alice" {
		t.Fatalf("expected createResult with user alice, got %s", createRes.Results[0].User)
	}

	describeCreationRes, err := client.DescribeUserScramCredentials(context.Background(), &DescribeUserScramCredentialsRequest{
		Users: []UserScramCredentialsUser{
			{
				Name: "alice",
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
				User: "alice",
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
			UserScramCredentialsDeletion{
				Name:      "alice",
				Mechanism: ScramMechanismSha512,
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	for _, err := range deleteRes.Errors {
		if err != nil {
			t.Error(err)
		}
	}

	if len(deleteRes.Results) != 1 {
		t.Fatalf("expected 1 deleteResult; got %d", len(deleteRes.Results))
	}

	if deleteRes.Results[0].User != "alice" {
		t.Fatalf("expected deleteResult with user alice, got %s", deleteRes.Results[0].User)
	}

	describeDeletionRes, err := client.DescribeUserScramCredentials(context.Background(), &DescribeUserScramCredentialsRequest{
		Users: []UserScramCredentialsUser{
			{
				Name: "alice",
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	if describeDeletionRes.Error != makeError(0, "") {
		t.Fatalf("didn't expect a top level error on describe results after deletion, got %v", describeDeletionRes.Error)
	}

	if len(describeDeletionRes.Results) != 1 {
		t.Fatalf("expected one describe results after deletion, got %d describe results", len(describeDeletionRes.Results))
	}

	result := describeDeletionRes.Results[0]

	if result.User != "alice" {
		t.Fatalf("expected describeResult with user alice, got %s", result.User)
	}

	if len(result.CredentialInfos) != 0 {
		t.Fatalf("didn't expect describeResult credential infos, got %v", result.CredentialInfos)
	}
}
