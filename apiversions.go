package kafka

import (
	"context"
	"net"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/apiversions"
)

type ApiVersionsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr
}

type ApiVersionsResponse struct {
	ErrorCode int16
	ApiKeys   []ApiVersionsResponseApiKey
}

type ApiVersionsResponseApiKey struct {
	ApiKey     int16
	ApiName    string
	MinVersion int16
	MaxVersion int16
}

func (c *Client) ApiVersions(
	ctx context.Context,
	req ApiVersionsRequest,
) (*ApiVersionsResponse, error) {
	apiReq := &apiversions.Request{}
	protoResp, err := c.roundTrip(
		ctx,
		req.Addr,
		apiReq,
	)
	apiResp := protoResp.(*apiversions.Response)

	resp := &ApiVersionsResponse{
		ErrorCode: apiResp.ErrorCode,
	}
	for _, apiKey := range apiResp.ApiKeys {
		resp.ApiKeys = append(
			resp.ApiKeys,
			ApiVersionsResponseApiKey{
				ApiKey:     apiKey.ApiKey,
				ApiName:    protocol.ApiKey(apiKey.ApiKey).String(),
				MinVersion: apiKey.MinVersion,
				MaxVersion: apiKey.MaxVersion,
			},
		)
	}

	return resp, err
}
