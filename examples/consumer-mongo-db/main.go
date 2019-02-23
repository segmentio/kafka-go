package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/mongodb/mongo-go-driver/mongo"
	kafka "github.com/segmentio/kafka-go"
)

func main() {
	mongoURL := os.Getenv("mongoURL")
	client, err := mongo.Connect(context.Background(), mongoURL)

	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.Background(), nil)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB ... !!")

	dbName := os.Getenv("dbName")
	collectionName := os.Getenv("collectionName")
	collection := client.Database(dbName).Collection(collectionName)

	fmt.Println("start consuming ... !!")
	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")
	groupID := os.Getenv("groupID")

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err)
			break
		}
		// msg := Data{data: m}
		insertResult, err := collection.InsertOne(context.Background(), msg)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Inserted a single document: ", insertResult.InsertedID)
	}

	r.Close()

}
