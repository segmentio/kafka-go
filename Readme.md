# Kafka Go

Kafka library in Go. This builds on Sarama but offers a few differences:

- Doesn't use Kafka consumer groups.
- Exposes a stream-based API
- Builds a low-level interface that can be composed with higher-level components.

# Getting Started

```golang
import "github.com/segmentio/kafka-go"

ctx := context.Background()

reader, err := kafka.NewReader(ReaderConfig{
  BrokerAddrs: []string{"localhost:9092"},
  Topic: "events",
  Partition: 0,
})

if err != nil {
  panic(err)
}

offset, err := reader.Seek(ctx, OffsetOldest)
if err != nil {
  panic(err)
}

for {
  msg, err := reader.Read(ctx)
  if err != nil {
    panic(err)
  }

  fmt.Println("key: %s value: %s offset: %d", msg.Key, msg.Value, msg.Offset)
}

reader.Close()
```

## Running Tests

```bash
govendor test +local
```

# License

MIT
