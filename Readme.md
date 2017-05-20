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
  Brokers: []string{"localhost:9092"},
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

### Creating a Group Reader

Instead of manually assigning partitions to each reader we can automate it with groups. Just give it the number of partitions there are and each reader
will take ownership of one.

```golang
config := kafka.GroupConfig{
  Name: "my-group-name",
  // Consul address
  Addr: "http://localhost:8500",
  Brokers: []string{"localhost:9092"},
  Topic: "foobar",
  Partitions: 100,

  // ...
}

reader, err := kafka.NewGroupReader(config)
if err != nil {
  panic(err)
}

offset, err := reader.Seek(ctx, kafka.OffsetOldest)
// ...

msg, err := reader.Read(ctx)
// ...

defer reader.Close()
```

# Kafka CLI

Some utility tools exist that were built with this library.

```bash
$ go install github.com/segmentio/kafka-go/cmd/kafkacli
$ kafkacli
Usage:
  kafkacli [command] [options...]

Commands:
  tail     Tail a partition or topic
  help     Show the kafkacli help
  version  Show the kafkacli version

Error:
  missing command
```

### Tailing Messages

```bash
kafkacli tail -kafka-url kafka://localhost:9092/events
```

Where `events` is the topic you want to tail and `localhost:9092` is the broker.

## Running Tests

```bash
govendor test +local
```

# License

MIT
