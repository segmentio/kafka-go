package kafka

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go/protocol/describeconfigs"
)

type DescribeConfigsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	Topics          []string
	Brokers         []int
	IncludeDefaults bool
}

type DescribeConfigsResponse struct {
	Brokers []DescribeConfigsResponseBroker
	Topics  []DescribeConfigsResponseTopic
}

type DescribeConfigsResponseBroker struct {
	BrokerID int
	Configs  map[string]string
}

type DescribeConfigsResponseTopic struct {
	Topic   string
	Configs map[string]string
}

func (c *Client) DescribeConfigs(
	ctx context.Context,
	req DescribeConfigsRequest,
) (*DescribeConfigsResponse, error) {
	resources := []describeconfigs.RequestResource{}

	for _, broker := range req.Brokers {
		resources = append(
			resources,
			describeconfigs.RequestResource{
				ResourceType: describeconfigs.ResourceTypeBroker,
				ResourceName: fmt.Sprintf("%d", broker),
			},
		)
	}

	for _, topic := range req.Topics {
		resources = append(
			resources,
			describeconfigs.RequestResource{
				ResourceType: describeconfigs.ResourceTypeTopic,
				ResourceName: topic,
			},
		)
	}

	protoResp, err := c.roundTrip(
		ctx,
		req.Addr,
		&describeconfigs.Request{
			Resources: resources,
		},
	)
	if err != nil {
		return nil, err
	}
	apiResp := protoResp.(*describeconfigs.Response)

	resp := &DescribeConfigsResponse{}

	for _, resource := range apiResp.Resources {
		switch resource.ResourceType {
		case describeconfigs.ResourceTypeBroker:
			configs := map[string]string{}
			for _, entry := range resource.ConfigEntries {
				if !req.IncludeDefaults && (entry.IsDefault ||
					entry.ConfigSource == describeconfigs.ConfigSourceDefaultConfig ||
					entry.ConfigSource == describeconfigs.ConfigSourceStaticBrokerConfig ||
					entry.ConfigSource == describeconfigs.ConfigSourceDynamicDefaultBrokerConfig) {
					continue
				}

				configs[entry.ConfigName] = entry.ConfigValue
			}

			brokerID, err := strconv.Atoi(resource.ResourceName)
			if err != nil {
				return nil, err
			}

			resp.Brokers = append(resp.Brokers, DescribeConfigsResponseBroker{
				BrokerID: brokerID,
				Configs:  configs,
			})
		case describeconfigs.ResourceTypeTopic:
			configs := map[string]string{}
			for _, entry := range resource.ConfigEntries {
				if !req.IncludeDefaults && (entry.IsDefault ||
					entry.ConfigSource == describeconfigs.ConfigSourceDefaultConfig ||
					entry.ConfigSource == describeconfigs.ConfigSourceStaticBrokerConfig) {
					continue
				}

				configs[entry.ConfigName] = entry.ConfigValue
			}

			resp.Topics = append(resp.Topics, DescribeConfigsResponseTopic{
				Topic:   resource.ResourceName,
				Configs: configs,
			})
		default:
			return nil, fmt.Errorf("Unrecognized resource type: %d", resource.ResourceType)
		}
	}

	return resp, nil
}
