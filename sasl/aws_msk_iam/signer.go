package aws_msk_iam

import (
	"context"
	"errors"
	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	sigv2 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	sig "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/segmentio/kafka-go/sasl"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type AWSSignerV1 struct {
	// The sig.Signer to use when signing the request
	Signer *sig.Signer
}

type AWSSignerV2 struct {
	// The sigv2.Signer to use when signing the request. Required.
	Signer *sigv2.Signer
	// The aws.Credentials of aws-sdk-go-v2. Required.
	Credentials awsv2.Credentials
}

// buildReq builds http.Request for aws PreSign.
func buildReq(ctx context.Context, query url.Values) (*http.Request, error) {
	saslMeta := sasl.MetadataFromContext(ctx)
	if saslMeta == nil {
		return nil, errors.New("missing sasl metadata")
	}

	signUrl := url.URL{
		Scheme:   "kafka",
		Host:     saslMeta.Host,
		Path:     "/",
		RawQuery: query.Encode(),
	}

	req, err := http.NewRequest(http.MethodGet, signUrl.String(), nil)
	if err != nil {
		return nil, err
	}

	return req, nil
}

// buildSignedMap builds signed string map
func buildSignedMap(u *url.URL, header http.Header) map[string]string {
	signedMap := map[string]string{
		signVersionKey:   signVersion,
		signHostKey:      u.Host,
		signUserAgentKey: signUserAgent,
		signActionKey:    signAction,
	}
	// The protocol requires lowercase keys.
	for key, vals := range header {
		signedMap[strings.ToLower(key)] = vals[0]
	}
	for key, vals := range u.Query() {
		signedMap[strings.ToLower(key)] = vals[0]
	}

	return signedMap
}

func defaultExpiry(v time.Duration) time.Duration {
	if v == 0 {
		return 5 * time.Minute
	}
	return v
}

func defaultSignTime(v time.Time) time.Time {
	if v.IsZero() {
		return time.Now()
	}
	return v
}

func (s *AWSSignerV1) PreSign(ctx context.Context, expiry time.Duration, signTime time.Time, region string) (map[string]string, error) {
	query := url.Values{
		queryActionKey: {signAction},
	}
	req, err := buildReq(ctx, query)
	if err != nil {
		return nil, err
	}

	header, err := s.Signer.Presign(req, nil, signService, region, defaultExpiry(expiry), defaultSignTime(signTime))
	if err != nil {
		return nil, err
	}

	return buildSignedMap(req.URL, header), nil
}

func (s *AWSSignerV2) PreSign(ctx context.Context, expiry time.Duration, signTime time.Time, region string) (map[string]string, error) {

	query := url.Values{
		queryActionKey: {signAction},
		queryExpiryKey: {strconv.FormatInt(int64(defaultExpiry(expiry)/time.Second), 10)},
	}

	req, err := buildReq(ctx, query)
	if err != nil {
		return nil, err
	}

	signedUrl, header, err := s.Signer.PresignHTTP(ctx, s.Credentials, req, signPayload, signService, region, defaultSignTime(signTime))
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(signedUrl)
	if err != nil {
		return nil, err
	}

	return buildSignedMap(u, header), nil
}
