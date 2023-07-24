package alteruserscramcredentials_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/alteruserscramcredentials"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0 = 0
)

func TestAlterUserScramCredentialsRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &alteruserscramcredentials.Request{
		Deletions: []alteruserscramcredentials.RequestUserScramCredentialsDeletion{
			{
				Name:      "foo-1",
				Mechanism: 1,
			},
		},
		Upsertions: []alteruserscramcredentials.RequestUserScramCredentialsUpsertion{
			{
				Name:           "foo-2",
				Mechanism:      2,
				Iterations:     15000,
				Salt:           []byte("my-salt"),
				SaltedPassword: []byte("my-salted-password"),
			},
		},
	})
}

func TestAlterUserScramCredentialsResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &alteruserscramcredentials.Response{
		ThrottleTimeMs: 500,
		Results: []alteruserscramcredentials.ResponseUserScramCredentials{
			{
				User:         "foo",
				ErrorCode:    1,
				ErrorMessage: "foo-error",
			},
		},
	})
}
