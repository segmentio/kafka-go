package aws_msk_iam

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"time"

	sig "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/segmentio/kafka-go/sasl"
)

const (
	// These constants come from https://github.com/aws/aws-msk-iam-auth#details and
	// https://github.com/aws/aws-msk-iam-auth/blob/main/src/main/java/software/amazon/msk/auth/iam/internals/AWS4SignedPayloadGenerator.java.
	signAction       = "kafka-cluster:Connect"
	signPayload      = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // the hex encoded SHA-256 of an empty string
	signService      = "kafka-cluster"
	signVersion      = "2020_10_22"
	signActionKey    = "action"
	signHostKey      = "host"
	signUserAgentKey = "user-agent"
	signVersionKey   = "version"
	queryActionKey   = "Action"
	queryExpiryKey   = "X-Amz-Expires"
)

var signUserAgent = fmt.Sprintf("kafka-go/sasl/aws_msk_iam/%s", runtime.Version())

type SignerIfc interface {
	PreSign(ctx context.Context, expiry time.Duration, signTime time.Time, region string) (map[string]string, error)
}

// Mechanism implements sasl.Mechanism for the AWS_MSK_IAM mechanism, based on the official java implementation:
// https://github.com/aws/aws-msk-iam-auth
type Mechanism struct {
	// Deprecated, to support both of the aws-sdk-go-v1 and aws-sdk-go-v2, we implemented GenericSigner. The sig.Signer to use when signing the request.
	Signer *sig.Signer
	// interface which supports both of the aws-sdk-go-v1 and aws-sdk-go-v2, use when signing the request.
	GenericSigner SignerIfc
	// The region where the msk cluster is hosted, e.g. "us-east-1". Required.
	Region string
	// The time the request is planned for. Optional, defaults to time.Now() at time of authentication.
	SignTime time.Time
	// The duration for which the presigned request is active. Optional, defaults to 5 minutes.
	Expiry time.Duration
}

func (m *Mechanism) Name() string {
	return "AWS_MSK_IAM"
}

// Start produces the authentication values required for AWS_MSK_IAM. It produces the following json as a byte array,
// making use of the aws-sdk to produce the signed output.
// 	{
// 	  "version" : "2020_10_22",
// 	  "host" : "<broker host>",
// 	  "user-agent": "<user agent string from the client>",
// 	  "action": "kafka-cluster:Connect",
// 	  "x-amz-algorithm" : "<algorithm>",
// 	  "x-amz-credential" : "<clientAWSAccessKeyID>/<date in yyyyMMdd format>/<region>/kafka-cluster/aws4_request",
// 	  "x-amz-date" : "<timestamp in yyyyMMdd'T'HHmmss'Z' format>",
// 	  "x-amz-security-token" : "<clientAWSSessionToken if any>",
// 	  "x-amz-signedheaders" : "host",
// 	  "x-amz-expires" : "<expiration in seconds>",
// 	  "x-amz-signature" : "<AWS SigV4 signature computed by the client>"
// 	}
func (m *Mechanism) Start(ctx context.Context) (sess sasl.StateMachine, ir []byte, err error) {
	if m.GenericSigner == nil && m.Signer == nil {
		return nil, nil, fmt.Errorf("no genSigner provided to Mechanism")
	}

	// Keep backward compatibility, use AWSSignerV1 if GenericSigner is not defined
	if m.GenericSigner == nil {
		m.GenericSigner = &AWSSignerV1{Signer: m.Signer}
	}

	signedMap, err := m.GenericSigner.PreSign(ctx, m.Expiry, m.SignTime, m.Region)
	if err != nil {
		return nil, nil, err
	}

	signedJson, err := json.Marshal(signedMap)
	return m, signedJson, err
}

func (m *Mechanism) Next(ctx context.Context, challenge []byte) (bool, []byte, error) {
	// After the initial step, the authentication is complete
	// kafka will return error if it rejected the credentials, so we'll only
	// arrive here on success.
	return true, nil, nil
}
