package aws_msk_iam

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/sasl"

	sigv2 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	credentialsv2 "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials"
	sig "github.com/aws/aws-sdk-go/aws/signer/v4"
)

const (
	accessKeyId     = "ACCESS_KEY"
	secretAccessKey = "SECRET_KEY"
)

// using a fixed time allows the signature to be verifiable in a test
var signTime = time.Date(2021, 10, 14, 13, 5, 0, 0, time.UTC)

func TestAwsMskIamMechanism(t *testing.T) {
	credsV1 := credentials.NewStaticCredentials(accessKeyId, secretAccessKey, "")
	credsV2, err := credentialsv2.NewStaticCredentialsProvider(accessKeyId, secretAccessKey, "").Retrieve(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	ctxWithMetadata := func() context.Context {
		return sasl.WithMetadata(context.Background(), &sasl.Metadata{
			Host: "localhost",
			Port: 9092,
		})

	}

	tests := []struct {
		description string
		ctx         func() context.Context
		signer      *sig.Signer
		genSigner   SignerIfc
		shouldFail  bool
	}{
		{
			description: "with metadata",
			ctx:         ctxWithMetadata,
			signer:      sig.NewSigner(credsV1),
		},
		{
			description: "without metadata",
			ctx: func() context.Context {
				return context.Background()
			},
			signer:     sig.NewSigner(credsV1),
			shouldFail: true,
		},
		{
			description: "v1 generic signer",
			ctx:         ctxWithMetadata,
			genSigner:   &AWSSignerV1{Signer: sig.NewSigner(credsV1)},
		},
		{
			description: "v2 generic signer",
			ctx:         ctxWithMetadata,
			genSigner:   &AWSSignerV2{Signer: sigv2.NewSigner(), Credentials: credsV2},
		},
		{
			description: "no signer",
			ctx:         ctxWithMetadata,
			shouldFail:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			ctx := tt.ctx()

			mskMechanism := &Mechanism{
				Signer:        tt.signer,
				GenericSigner: tt.genSigner,
				Region:        "us-east-1",
				SignTime:      signTime,
			}

			sess, auth, err := mskMechanism.Start(ctx)
			if tt.shouldFail { // if error is expected
				if err == nil { // but we don't find one
					t.Fatal("error expected")
				} else { // but we do find one
					return // return early since the remaining assertions are irrelevant
				}
			} else { // if error is not expected (typical)
				if err != nil { // but we do find one
					t.Fatal(err)
				}
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
		})
	}
}
