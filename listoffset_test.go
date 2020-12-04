package kafka

import (
	"context"
	"testing"
	"time"
)

func TestClientListOffsets(t *testing.T) {
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

	res, err := client.ListOffsets(context.Background(), &ListOffsetsRequest{
		Topics: map[string][]OffsetRequest{
			topic: {FirstOffsetOf(0), LastOffsetOf(0)},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(res.Topics) != 1 {
		t.Fatal("invalid number of topics found in list offsets response:", len(res.Topics))
	}

	partitions, ok := res.Topics[topic]
	if !ok {
		t.Fatal("missing topic in the list offsets response:", topic)
	}
	if len(partitions) != 1 {
		t.Fatal("invalid number of partitions found in list offsets response:", len(partitions))
	}
	partition := partitions[0]

	if partition.Partition != 0 {
		t.Error("invalid partition id found in list offsets response:", partition.Partition)
	}

	if partition.FirstOffset != 0 {
		t.Error("invalid first offset found in list offsets response:", partition.FirstOffset)
	}

	if partition.LastOffset != 3 {
		t.Error("invalid last offset found in list offsets response:", partition.LastOffset)
	}

	if firstOffsetTime := partition.Offsets[partition.FirstOffset]; !firstOffsetTime.IsZero() {
		t.Error("unexpected first offset time in list offsets response:", partition.Offsets)
	}

	if lastOffsetTime := partition.Offsets[partition.LastOffset]; !lastOffsetTime.IsZero() {
		t.Error("unexpected last offset time in list offsets response:", partition.Offsets)
	}

	if partition.Error != nil {
		t.Error("unexpected error in list offsets response:", partition.Error)
	}
}
