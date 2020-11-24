package kafka

import (
	"context"
	"testing"
)

func TestClientAlterConfigs(t *testing.T) {

	client, shutdown := newLocalClient()
	defer shutdown()

	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	res, err := client.AlterConfigs(context.Background(), &AlterConfigsRequest{
		Resources: []AlterConfigRequestResource{{
			ResourceType: ResourceTypeTopic,
			ResourceName: topic,
			Configs: []AlterConfigRequestConfig{{
				Name:  "max.message.bytes",
				Value: "200000",
			},
			},
		}},
	})

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Client.DescribeConfigs() = %v", res)
}
