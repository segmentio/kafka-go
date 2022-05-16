package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/segmentio/kafka-go"
)

func ExampleGeneration_Start_consumerGroupParallelReaders() {
	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      "my-group",
		Brokers: []string{"kafka:9092"},
		Topics:  []string{"my-topic"},
	})
	if err != nil {
		fmt.Printf("error creating consumer group: %+v\n", err)
		os.Exit(1)
	}
	defer group.Close()

	for {
		gen, err := group.Next(context.TODO())
		if err != nil {
			break
		}

		assignments := gen.Assignments["my-topic"]
		for _, assignment := range assignments {
			partition, offset := assignment.ID, assignment.Offset
			gen.Start(func(ctx context.Context) {
				// create reader for this partition.
				reader := kafka.NewReader(kafka.ReaderConfig{
					Brokers:   []string{"127.0.0.1:9092"},
					Topic:     "my-topic",
					Partition: partition,
				})
				defer reader.Close()

				// seek to the last committed offset for this partition.
				reader.SetOffset(offset)
				for {
					msg, err := reader.ReadMessage(ctx)
					if err != nil {
						if errors.Is(err, kafka.ErrGenerationEnded) {
							// generation has ended.  commit offsets.  in a real app,
							// offsets would be committed periodically.
							gen.CommitOffsets(map[string]map[int]int64{"my-topic": {partition: offset + 1}})
							return
						}

						fmt.Printf("error reading message: %+v\n", err)
						return
					}

					fmt.Printf("received message %s/%d/%d : %s\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
					offset = msg.Offset
				}
			})
		}
	}
}

func ExampleGeneration_CommitOffsets_overwriteOffsets() {
	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      "my-group",
		Brokers: []string{"kafka:9092"},
		Topics:  []string{"my-topic"},
	})
	if err != nil {
		fmt.Printf("error creating consumer group: %+v\n", err)
		os.Exit(1)
	}
	defer group.Close()

	gen, err := group.Next(context.TODO())
	if err != nil {
		fmt.Printf("error getting next generation: %+v\n", err)
		os.Exit(1)
	}
	err = gen.CommitOffsets(map[string]map[int]int64{
		"my-topic": {
			0: 123,
			1: 456,
			3: 789,
		},
	})
	if err != nil {
		fmt.Printf("error committing offsets next generation: %+v\n", err)
		os.Exit(1)
	}
}
