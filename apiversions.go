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
	ErrorCode int
	ApiKeys   []ApiVersionsResponseApiKey
}

type ApiVersionsResponseApiKey struct {
	ApiKey     int
	ApiName    string
	MinVersion int
	MaxVersion int
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
	if err != nil {
		return nil, err
	}
	apiResp := protoResp.(*apiversions.Response)

	resp := &ApiVersionsResponse{
		ErrorCode: int(apiResp.ErrorCode),
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
