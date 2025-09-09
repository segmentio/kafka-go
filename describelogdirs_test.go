package kafka

import (
	"context"
	"testing"
	"time"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestDescribeLogDirs(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("1.0.1") {
		return
	}

	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	now := time.Now()

	_, err := client.Produce(context.Background(), &ProduceRequest{
		Topic:        topic,
		Partition:    0,
		RequiredAcks: -1,
		Records: NewRecordReader(
			Record{Time: now, Value: NewBytes([]byte(`hello-1`))},
			Record{Time: now, Value: NewBytes([]byte(`hello-2`))},
			Record{Time: now, Value: NewBytes([]byte(`hello-3`))},
		),
	})

	if err != nil {
		t.Fatal(err)
	}

	res, err := client.DescribeLogDirs(context.Background(), &DescribeLogDirsRequest{})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Results) == 0 {
		t.Error("no brokers were returned in the describeLogDirs response")
	}

	for _, logDirs := range res.Results {
		if len(logDirs) == 0 {
			t.Error("no broker log dirs were returned in the describeLogDirs response")
		}

		for _, description := range logDirs {
			if len(description.ReplicaInfos) == 0 {
				t.Error("no replica information were returned in the describeLogDirs response")
			}
		}
	}
}
