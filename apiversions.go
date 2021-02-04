package kafka

import (
	"context"
	"fmt"
	"net"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/apiversions"
)

// ApiVersionsRequest is a request to the ApiVersions API.
type ApiVersionsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr
}

// ApiVersionsResponse is a response from the ApiVersions API.
type ApiVersionsResponse struct {
	// Error is set to a non-nil value if an error was encountered.
	Error error

	// ApiKeys contains the specific details of each supported API.
	ApiKeys []ApiVersionsResponseApiKey
}

// ApiVersionsResponseApiKey includes the details of which versions are supported for a single API.
type ApiVersionsResponseApiKey struct {
	// ApiKey is the ID of the API.
	ApiKey int

	// ApiName is a human-friendly description of the API.
	ApiName string

	// MinVersion is the minimum API version supported by the broker.
	MinVersion int

	// MaxVersion is the maximum API version supported by the broker.
	MaxVersion int
}

func (c *Client) ApiVersions(
	ctx context.Context,
	req *ApiVersionsRequest,
) (*ApiVersionsResponse, error) {
	apiReq := &apiversions.Request{}
	protoResp, err := c.roundTrip(
		ctx,
		req.Addr,
		apiReq,
	)
	if err != nil {
		return nil, err
	}
	apiResp := protoResp.(*apiversions.Response)

	resp := &ApiVersionsResponse{
		Error: makeError(apiResp.ErrorCode, ""),
	}
	for _, apiKey := range apiResp.ApiKeys {
		resp.ApiKeys = append(
			resp.ApiKeys,
			ApiVersionsResponseApiKey{
				ApiKey:     int(apiKey.ApiKey),
				ApiName:    protocol.ApiKey(apiKey.ApiKey).String(),
				MinVersion: int(apiKey.MinVersion),
				MaxVersion: int(apiKey.MaxVersion),
			},
		)
	}

	return resp, err
}

// IsApiKeySupported checks if the API key is supported by the broker
func (c *Client) IsApiKeySupported(
	ctx context.Context,
	key protocol.ApiKey) (bool, error) {
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
			keySupported = true
			break
		}
	}
	return keySupported, nil
}
