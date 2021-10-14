package aws_msk_iam

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	sigv4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/segmentio/kafka-go/sasl"
)

const (
	// These constants come from https://github.com/aws/aws-msk-iam-auth#details and
	// https://github.com/aws/aws-msk-iam-auth/blob/main/src/main/java/software/amazon/msk/auth/iam/internals/AWS4SignedPayloadGenerator.java.
	signVersion      = "2020_10_22"
	signService      = "kafka-cluster"
	signAction       = "kafka-cluster:Connect"
	signVersionKey   = "version"
	signHostKey      = "host"
	signUserAgentKey = "user-agent"
	signActionKey    = "action"
	queryActionKey   = "Action"

	defaultSignExpiry = 5 * time.Minute
)

var signUserAgent = fmt.Sprintf("kafka-go/sasl/aws_msk_iam/%s", runtime.Version())

// Mechanism implements sasl.Mechanism for the AWS_MSK_IAM mechanism, based on the official java implementation:
// https://github.com/aws/aws-msk-iam-auth
type Mechanism struct {
	Signer *sigv4.Signer
	// The host of the kafka broker to connect to.
	BrokerHost string
	// The region where the msk cluster is hosted.
	AwsRegion string
	// The time the request is planned for. Defaults to time.Now() at time of authentication.
	SignTime time.Time
	// The duration for which the presigned-request is active. Defaults to 15 minutes.
	Expiry time.Duration
}

// NewMechanism creates a sasl.Mechanism for AWS_MSK_IAM
func NewMechanism(brokerHost, awsRegion string, creds *credentials.Credentials) *Mechanism {
	return &Mechanism{
		BrokerHost: brokerHost,
		AwsRegion:  awsRegion,
		Signer:     sigv4.NewSigner(creds),
		Expiry:     defaultSignExpiry,
	}
}

func (m *Mechanism) Name() string {
	return "AWS_MSK_IAM"
}

// Start produces the authentication values required for AWS_MSK_IAM. It produces the following json as a byte array,
// making use of the aws-sdk to produce the signed output.
//{
//	"version" : "2020_10_22",
//	"host" : "<broker address>",
//	"user-agent": "<user agent string from the client>",
//	"action": "kafka-cluster:Connect",
//	"x-amz-algorithm" : "<algorithm>",
//	"x-amz-credential" : "<clientAWSAccessKeyID>/<date in yyyyMMdd format>/<region>/kafka-cluster/aws4_request",
//	"x-amz-date" : "<timestamp in yyyyMMdd'T'HHmmss'Z' format>",
//	"x-amz-security-token" : "<clientAWSSessionToken if any>",
//	"x-amz-signedheaders" : "host",
//	"x-amz-expires" : "<expiration in seconds>",
//	"x-amz-signature" : "<AWS SigV4 signature computed by the client>"
//}
func (m *Mechanism) Start(ctx context.Context) (sess sasl.StateMachine, ir []byte, err error) {
	// The trailing slash and protocol are necessary here.
	// The sigv4.Signer will take the host and the path from the url.
	// The host will be the broker host, and the path will be "/".
	url := fmt.Sprintf("kafka://%s/?%s=%s", m.BrokerHost, queryActionKey, signAction)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, nil, err
	}

	signTime := m.SignTime
	if signTime.IsZero() {
		signTime = time.Now()
	}

	header, err := m.Signer.Presign(req, nil, signService, m.AwsRegion, m.Expiry, signTime)
	if err != nil {
		return nil, nil, err
	}
	signedMap := map[string]string{
		signVersionKey:   signVersion,
		signHostKey:      m.BrokerHost,
		signUserAgentKey: signUserAgent,
		signActionKey:    signAction,
	}
	// The protocol requires lowercase keys.
	for key, vals := range header {
		signedMap[strings.ToLower(key)] = vals[0]
	}
	for key, vals := range req.URL.Query() {
		signedMap[strings.ToLower(key)] = vals[0]
	}

	signedJson, err := json.Marshal(signedMap)
	if err != nil {
		return nil, nil, err
	}
	return m, signedJson, nil
}

func (m *Mechanism) Next(ctx context.Context, challenge []byte) (bool, []byte, error) {
	// After the initial step, the authentication is complete
	// kafka will return error if it rejected the credentials, so we'll only
	// arrive here on success.
	return true, nil, nil
}
