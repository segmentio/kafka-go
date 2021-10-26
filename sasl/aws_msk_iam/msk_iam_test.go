package aws_msk_iam

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	sigv4 "github.com/aws/aws-sdk-go/aws/signer/v4"
)

const (
	accessKeyId     = "ACCESS_KEY"
	secretAccessKey = "SECRET_KEY"
)

// using a fixed time allows the signature to be verifiable in a test
var signTime = time.Date(2021, 10, 14, 13, 5, 0, 0, time.UTC)

func TestAwsMskIamMechanism(t *testing.T) {
	tests := []struct {
		ctx func() context.Context
	}{
		{
			ctx: func() context.Context {
				return context.WithValue(context.Background(),
					kafka.ContextKeyBrokerAddr, "localhost:9092")
			},
		},
		{
			ctx: func() context.Context {
				return context.WithValue(context.Background(),
					kafka.ContextKeyBrokerAddr, "localhost")
			},
		},
	}
	for _, tt := range tests {
		ctx := tt.ctx()

		creds := credentials.NewStaticCredentials(accessKeyId, secretAccessKey, "")
		mskMechanism := &Mechanism{
			Signer:   sigv4.NewSigner(creds),
			Region:   "us-east-1",
			SignTime: signTime,
		}

		sess, auth, err := mskMechanism.Start(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if sess != mskMechanism {
			t.Error(
				"Unexpected session",
				"expected", mskMechanism,
				"got", sess,
			)
		}

		expectedMap := map[string]string{
			"version":             "2020_10_22",
			"action":              "kafka-cluster:Connect",
			"host":                "localhost",
			"user-agent":          signUserAgent,
			"x-amz-algorithm":     "AWS4-HMAC-SHA256",
			"x-amz-credential":    "ACCESS_KEY/20211014/us-east-1/kafka-cluster/aws4_request",
			"x-amz-date":          "20211014T130500Z",
			"x-amz-expires":       "300",
			"x-amz-signedheaders": "host",
			"x-amz-signature":     "6b8d25f9b45b9c7db9da855a49112d80379224153a27fd279c305a5b7940d1a7",
		}
		expectedAuth, err := json.Marshal(expectedMap)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(expectedAuth, auth) {
			t.Error("Unexpected authentication",
				"expected", expectedAuth,
				"got", auth,
			)
		}
	}

}
