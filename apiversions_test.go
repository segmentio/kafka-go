package kafka

import (
	"context"
	"fmt"
	"testing"

	"github.com/segmentio/kafka-go/protocol"
)

func TestClientApiVersions(t *testing.T) {
	ctx := context.Background()

	client, shutdown := newLocalClient()
	defer shutdown()

	resp, err := client.ApiVersions(ctx, &ApiVersionsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error != nil {
		t.Error(
			"Unexpected error in response",
			"expected", nil,
			"got", resp.Error,
		)
	}

	if len(resp.ApiKeys) == 0 {
		t.Error(
			"Unexpected apiKeys length",
			"expected greater than", 0,
			"got", 0,
		)
	}
}

// IsApiKeySupported checks if the API key is supported by the broker
func isAPIKeySupported(ctx context.Context, c *Client, key protocol.ApiKey) (bool, error) {
	supportedKeys, err := c.ApiVersions(
		ctx,
		&ApiVersionsRequest{
			c.Addr,
		},
	)
	if err != nil {
		return false, fmt.Errorf("kafka.(*Client).IsApiKeySupported: %w", err)
	}
	keySupported := false
	for _, k := range supportedKeys.ApiKeys {
		if int(k.ApiKey) == int(key) {
			return true, nil
		}
	}
	return keySupported, nil
}
