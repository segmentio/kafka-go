# Set Consumer Group Offset Example

This example demonstrates how to use the `SetConsumerGroupOffset` functionality to explicitly set Kafka consumer group offsets.

## Overview

The `SetConsumerGroupOffset` function allows you to set offsets for a consumer group without being part of an active consumer group session. This is particularly useful for:

- Administrative tasks
- Resetting consumer group offsets
- Setting initial offsets for new consumer groups
- Manual offset management

## Features

1. **Automatic Session Management**: If no generation ID or member ID is provided, the function automatically creates a temporary consumer group session to obtain valid IDs required by the Kafka protocol.

2. **Explicit Session Support**: You can also provide your own generation and member IDs if you already have an active consumer group session.

3. **Error Handling**: The function provides detailed error information for each partition.

## Usage

### Example 1: Setting offsets without active session

```go
resp, err := client.SetConsumerGroupOffset(ctx, &kafka.SetConsumerGroupOffsetRequest{
    GroupID:      "my-consumer-group",
    GenerationID: 0, // Will be automatically set by the function
    MemberID:     "", // Will be automatically set by the function
    Topics: map[string][]kafka.SetOffset{
        "my-topic": {
            {Partition: 0, Offset: 100, Metadata: "admin-reset"},
            {Partition: 1, Offset: 150, Metadata: "admin-reset"},
            {Partition: 2, Offset: 200, Metadata: "admin-reset"},
        },
    },
})
```

### Example 2: Setting offsets with explicit session

```go
// Create a consumer group to get valid generation and member IDs
group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
    ID:                "my-consumer-group-2",
    Topics:            []string{"my-topic"},
    Brokers:           []string{"localhost:9092"},
    HeartbeatInterval: 2 * time.Second,
    RebalanceTimeout:  2 * time.Second,
    RetentionTime:     time.Hour,
})

// Get a generation to obtain valid IDs
gen, err := group.Next(ctx)

// Set offsets using the obtained generation and member IDs
resp, err := client.SetConsumerGroupOffset(ctx, &kafka.SetConsumerGroupOffsetRequest{
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
```

## Running the Example

1. Make sure you have a Kafka instance running (e.g., using the provided docker-compose.yml)
2. Navigate to this directory
3. Run the example:

```bash
go run main.go
```

## Prerequisites

- Go 1.16 or later
- Kafka instance running on localhost:9092
- A topic named "my-topic" with at least 3 partitions

## Notes

- The function automatically handles the creation of temporary consumer group sessions when needed
- All sessions are properly closed after use
- The function provides detailed error information for each partition
- Throttle time information is included in the response 
