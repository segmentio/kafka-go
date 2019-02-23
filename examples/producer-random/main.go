package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

func main() {
	fmt.Println("start producing ... !!")
	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaURL},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	i := 0
	for {
		i++
		w.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(fmt.Sprintf("Key-%d", i)),
				Value: []byte(fmt.Sprint(uuid.New())),
			})
		time.Sleep(1 * time.Second)
	}

	w.Close()
}
