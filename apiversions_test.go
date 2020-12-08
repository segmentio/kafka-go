package kafka

import (
	"context"
	"testing"
)

func TestClientApiVersions(t *testing.T) {
	ctx := context.Background()

	client, shutdown := newLocalClient()
	defer shutdown()

	resp, err := client.ApiVersions(ctx, &ApiVersionsRequest{})
	if err != nil {
		t.Fatal(err)
	}

	if resp.ErrorCode != 0 {
		t.Error(
			"Unexpected error code",
			"expected", 0,
			"got", resp.ErrorCode,
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
