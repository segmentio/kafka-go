package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	kafka "github.com/segmentio/kafka-go"
)

func producer(kafkaWriter *kafka.Writer) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Fatalln(err)
		}

		err = kafkaWriter.WriteMessages(req.Context(),
			kafka.Message{
				Key:   []byte(fmt.Sprintf("address-%s", req.RemoteAddr)),
				Value: body,
			})

		if err != nil {
			wrt.Write([]byte(err.Error()))
		}
	})
}

func main() {
	fmt.Println("start producer-api ... !!")

	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")

	kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaURL},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})

	// Add handle funcs for producer.
	http.HandleFunc("/", producer(kafkaWriter))

	// Run the web server.
	log.Fatal(http.ListenAndServe(":8080", nil))
}
