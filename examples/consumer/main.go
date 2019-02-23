package main

import (
	"context"
	"fmt"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	fmt.Println("start consuming ... !!")

	topic := "topic1"
	// make a new reader that consumes from topic1
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"kafka:9092"},
		GroupID:  "consumer-group-id",
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	r.Close()
}
