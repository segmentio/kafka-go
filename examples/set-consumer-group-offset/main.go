package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Create a Kafka client
	client := &kafka.Client{
		Addr: kafka.TCP("localhost:9092"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Example 1: Set offsets for a consumer group without being part of an active session
	// This is useful for administrative tasks or resetting consumer group offsets
	fmt.Println("Example 1: Setting consumer group offsets without active session")

	resp, err := client.SetConsumerGroupOffset(ctx, &kafka.SetConsumerGroupOffsetRequest{
		GroupID:      "my-consumer-group",
		GenerationID: 0,  // Will be automatically set by the function
		MemberID:     "", // Will be automatically set by the function
		Topics: map[string][]kafka.SetOffset{
			"my-topic": {
				{Partition: 0, Offset: 100, Metadata: "admin-reset"},
				{Partition: 1, Offset: 150, Metadata: "admin-reset"},
				{Partition: 2, Offset: 200, Metadata: "admin-reset"},
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to set consumer group offsets: %v", err)
	}

	fmt.Printf("Successfully set offsets for group 'my-consumer-group'\n")
	fmt.Printf("Throttle time: %v\n", resp.Throttle)

	for topicName, partitions := range resp.Topics {
		fmt.Printf("Topic: %s\n", topicName)
		for _, partition := range partitions {
			if partition.Error != nil {
				fmt.Printf("  Partition %d: ERROR - %v\n", partition.Partition, partition.Error)
			} else {
				fmt.Printf("  Partition %d: SUCCESS\n", partition.Partition)
			}
		}
	}

	// Example 2: Set offsets with explicit generation and member IDs
	// This is useful when you already have an active consumer group session
	fmt.Println("\nExample 2: Setting offsets with explicit generation and member IDs")

	// Create a consumer group to get valid generation and member IDs
	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:                "my-consumer-group-2",
		Topics:            []string{"my-topic"},
		Brokers:           []string{"localhost:9092"},
		HeartbeatInterval: 2 * time.Second,
		RebalanceTimeout:  2 * time.Second,
		RetentionTime:     time.Hour,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer group.Close()

	// Get a generation to obtain valid IDs
	gen, err := group.Next(ctx)
	if err != nil {
		log.Fatalf("Failed to get generation: %v", err)
	}

	// Set offsets using the obtained generation and member IDs
	resp2, err := client.SetConsumerGroupOffset(ctx, &kafka.SetConsumerGroupOffsetRequest{
		GroupID:      "my-consumer-group-2",
		GenerationID: int(gen.ID),
		MemberID:     gen.MemberID,
		Topics: map[string][]kafka.SetOffset{
			"my-topic": {
				{Partition: 0, Offset: 50, Metadata: "explicit-session"},
				{Partition: 1, Offset: 75, Metadata: "explicit-session"},
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to set consumer group offsets: %v", err)
	}

	fmt.Printf("Successfully set offsets for group 'my-consumer-group-2'\n")
	fmt.Printf("Throttle time: %v\n", resp2.Throttle)

	for topicName, partitions := range resp2.Topics {
		fmt.Printf("Topic: %s\n", topicName)
		for _, partition := range partitions {
			if partition.Error != nil {
				fmt.Printf("  Partition %d: ERROR - %v\n", partition.Partition, partition.Error)
			} else {
				fmt.Printf("  Partition %d: SUCCESS\n", partition.Partition)
			}
		}
	}

	fmt.Println("\nBoth examples completed successfully!")
}
