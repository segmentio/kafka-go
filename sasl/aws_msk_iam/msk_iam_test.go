package aws_msk_iam

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
)

const (
	accessKeyId     = "ACCESS_KEY"
	secretAccessKey = "SECRET_KEY"
)

// using a fixed time allows the signature to be verifiable in a test
var signTime = time.Date(2021, 10, 14, 13, 5, 0, 0, time.UTC)

func TestAwsMskIamMechanism(t *testing.T) {
	creds := credentials.NewStaticCredentials(accessKeyId, secretAccessKey, "")
	mskMechanism := NewMechanism("localhost", "us-east-1", creds)
	mskMechanism.SignTime = signTime

	sess, auth, err := mskMechanism.Start(context.Background())
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

	authMap := map[string]string{}
	err = json.Unmarshal(auth, &authMap)
	if err != nil {
		t.Fatal(err)
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

	if !reflect.DeepEqual(authMap, expectedMap) {
		t.Error("Unexpected authentication",
			"expected", expectedMap,
			"got", authMap,
		)
	}
}
