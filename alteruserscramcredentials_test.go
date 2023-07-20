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

	describeRes, err := client.DescribeUserScramCredentials(context.Background(), &DescribeUserScramCredentialsRequest{
		Users: []UserScramCredentialsUser{
			{
				Name: "alice",
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	expected := DescribeUserScramCredentialsResponse{
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

	assert.Equal(t, expected, *describeRes)
}
