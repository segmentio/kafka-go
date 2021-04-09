package kafka

import (
	"context"
	"reflect"
	"testing"

	ktesting "github.com/apoorvag-mav/kafka-go/testing"
)

func TestClientIncrementalAlterConfigs(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("2.4.0") {
		return
	}

	const (
		configKey   = "max.message.bytes"
		configValue = "200000"
	)

	ctx := context.Background()
	client, shutdown := newLocalClient()
	defer shutdown()

	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	resp, err := client.IncrementalAlterConfigs(
		ctx,
		&IncrementalAlterConfigsRequest{
			Resources: []IncrementalAlterConfigsRequestResource{
				{
					ResourceName: topic,
					ResourceType: ResourceTypeTopic,
					Configs: []IncrementalAlterConfigsRequestConfig{
						{
							Name:            configKey,
							Value:           configValue,
							ConfigOperation: ConfigOperationSet,
						},
					},
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	expRes := []IncrementalAlterConfigsResponseResource{
		{
			ResourceType: ResourceTypeTopic,
			ResourceName: topic,
		},
	}
	if !reflect.DeepEqual(expRes, resp.Resources) {
		t.Error(
			"Wrong response resources",
			"expected", expRes,
			"got", resp.Resources,
		)
	}

	dResp, err := client.DescribeConfigs(
		ctx,
		&DescribeConfigsRequest{
			Resources: []DescribeConfigRequestResource{
				{
					ResourceType: ResourceTypeTopic,
					ResourceName: topic,
					ConfigNames: []string{
						"max.message.bytes",
					},
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(dResp.Resources) != 1 || len(dResp.Resources[0].ConfigEntries) != 1 {
		t.Fatal("Invalid structure for DescribeResourcesResponse")
	}

	v := dResp.Resources[0].ConfigEntries[0].ConfigValue
	if v != configValue {
		t.Error(
			"Wrong altered value for max.message.bytes",
			"expected", configValue,
			"got", v,
		)
	}
}
