package aws_msk_iam_v2_test

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}
	mechanism := aws_msk_iam_v2.NewMechanism(cfg)
	_ = kafka.ReaderConfig{
		Brokers:     []string{"https://localhost"},
		GroupID:     "some-consumer-group",
		GroupTopics: []string{"some-topic"},
		Dialer: &kafka.Dialer{
			Timeout:       10 * time.Second,
			DualStack:     true,
			SASLMechanism: mechanism,
			TLS:           &tls.Config{},
		},
	}
}
