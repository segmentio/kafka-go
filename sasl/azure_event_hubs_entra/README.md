# Azure Event Hubs Entra

Provides support for [Azure Event Hub with Kafka Protocol](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-kafka-overview), 
using Azure Entra for authentication.

## How to use
This module is separate from the `kafka-go` module, since it is only required 
for Event Hub users.

You can add this module to your dependencies by running the command below:
```shell
go get github.com/segmentio/kafka-go/sasl/azure_event_hubs_entra
```

To connect to Event Hub with Kafka protocol:
```go
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/azure_event_hubs_entra"
)

func main() {
	// Create Azure Entra Default Credentials
	cred, err := azidentity.NewDefaultAzureCredential(nil)

	if err != nil {
		fmt.Printf("failed to create Default Azure Credential: %s", err.Error())
		os.Exit(1)
	}

	// Create Azure Entra SASL Mechanism
	entraMechanism := azure_event_hubs_entra.NewMechanism(cred)

	// Reader
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"<Event Hub Namespace Name>.servicebus.windows.net:9093"},
		GroupID: "<Arbitrary Consumer Group Id>",
		Topic:   "<Event Hub Name>",
		Dialer: &kafka.Dialer{
			SASLMechanism: entraMechanism,
			TLS:           &tls.Config{},
		},
	})

	defer r.Close()

	// Writer
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"<Event Hub Namespace Name>.servicebus.windows.net:9093"},
		Topic:   "<Event Hub Name>",
		Dialer: &kafka.Dialer{
			SASLMechanism: entraMechanism,
			TLS:           &tls.Config{},
		},
	})

	defer w.Close()

	err = w.WriteMessages(context.Background(), kafka.Message{
		Value: []byte("test"),
	})

	if err != nil {
		fmt.Printf("failed to write message: %s", err.Error())
		os.Exit(2)
	}

	message, err := r.ReadMessage(context.Background())

	if err != nil {
		fmt.Printf("failed to read message: %s", err.Error())
		os.Exit(3)
	}

	fmt.Printf("received message: %s", string(message.Value))
}

```