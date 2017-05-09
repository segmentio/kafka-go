# Kafka Go

(Mostly just some ideas on potential abstractions) -- Feel free to send PRs

Kafka library in Go. This builds on Sarama but offers a few differences:

- Doesn't use Kafka consumer groups.
- Exposes an iterator-based API
- Builds a low-level interface that can be composed with higher-level components.

# Getting Started

**Consuming messages from a single partition:**

```golang
import "github.com/segmentio/kafka-go"

reader, err := kafka.NewReader(ReaderConfig{
  BrokerAddrs: []string{"localhost:9092"},
  Topic: "events",
  Partition: 0,
})

if err != nil {
  panic(err)
}

iter := reader.Read(context.Background(), kafka.Offset(0))

var msg kafka.Message
for iter.Next(&msg) {
  fmt.Printf("offset: %d, key: %s, value: %s\n", int64(msg.Offset), string(msg.Key), string(msg.Value))
}
```

**Consuming from all partitions:**

```golang
import "github.com/segmentio/kafka-go"

group, err := kafka.NewConsulGroup(ConsulConfig{
  Name: "my-consumer-group",
  Addr: "localhost:8500",
})

reader, err := group.NewReader(ReaderConfig{
  BrokerAddrs: []string{"localhost:9092"},
  Topic: "events",
  Partitions: 100,
})
```

Each reader will consume from a single partition. If you have 100 partitions you would need 100 readers. These can be spread across instances if you want.

**Consume from multiple readers:**

Iterators are naturally composable.

```golang
import "github.com/segmentio/kafka-go"

group, err := kafka.NewConsulGroup(ConsulConfig{
  Name: "my-consumer-group",
  Addr: "localhost:8500",
})

readers := []kafka.Reader{}
iterators := []kakfa.MessageIter{}

for i := 0; i < 100; i++ {
  reader, err := group.NewReader(ReaderConfig{
    BrokerAddrs: []string{"localhost:9092"},
    Topic: "events",
    Partitions: 100,
  })

  iter := reader.Read(context.Background(), kafka.Offset(0))

  readers = append(readers, reader)
  iterators = append(iterators, iter)
}

iter := kafka.NewMultiIter(iterators)

var msg kafka.Message
for iter.Next(&msg) {
  // ...
}
```

**Spread partitions across consumers:**

So you have N consumers and K partitions that you want to spread around?

```golang
group, err := kafka.NewConsulGroup(ConsulConfig{
  Name: "my-consumer-group",
  Scheme: kafka.Spread
  Addr: "localhost:8500",
})

readers, err := group.NewReaders(...)

iterators := []kafka.MessageIter{}
for reader := range readers {
  iterators = append(iterators, reader.Read(context.Background(), kafka.Offset(0))
}

iter := kafka.NewMultiIter(iterators)

var msg kafka.Message
for iter.Next(&msg) {
  // ...
}
```

# License

MIT
