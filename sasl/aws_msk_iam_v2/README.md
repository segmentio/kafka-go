# AWS MSK IAM V2

This extension provides a capability to get authenticated with [AWS Managed Apache Kafka](https://aws.amazon.com/msk/)
through AWS IAM.

## How to use

This module is an extension for MSK users and thus this is isolated from `kafka-go` module.
You can add this module to your dependency by running the command below.

```shell
go get github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2
```

You can use the `Mechanism` for SASL authentication, like below.

```go
package main

import (
	"context"
	"crypto/tls"
	"time"

	signer "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
)

func main() {
	ctx := context.Background()

	// using aws-sdk-go-v2
	// NOTE: address error properly

	cfg, _ := awsCfg.LoadDefaultConfig(ctx)
	creds, _ := cfg.Credentials.Retrieve(ctx)
	m := &aws_msk_iam_v2.Mechanism{
		Signer:      signer.NewSigner(),
		Credentials: creds,
		Region:      "us-east-1",
		SignTime:    time.Now(),
		Expiry:      time.Minute * 5,
	}
	config := kafka.ReaderConfig{
		Brokers:     []string{"https://localhost"},
		GroupID:     "some-consumer-group",
		GroupTopics: []string{"some-topic"},
		Dialer: &kafka.Dialer{
			Timeout:       10 * time.Second,
			DualStack:     true,
			SASLMechanism: m,
			TLS:           &tls.Config{},
		},
	}
}


```