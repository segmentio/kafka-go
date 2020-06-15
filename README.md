# kafka-go [![CircleCI](https://circleci.com/gh/segmentio/kafka-go.svg?style=shield)](https://circleci.com/gh/segmentio/kafka-go) [![Go Report Card](https://goreportcard.com/badge/github.com/segmentio/kafka-go)](https://goreportcard.com/report/github.com/segmentio/kafka-go) [![GoDoc](https://godoc.org/github.com/segmentio/kafka-go?status.svg)](https://godoc.org/github.com/segmentio/kafka-go)

## Motivations

We rely on both Go and Kafka a lot at Segment. Unfortunately, the state of the Go
client libraries for Kafka at the time of this writing was not ideal. The available
options were:

- [sarama](https://github.com/Shopify/sarama), which is by far the most popular
but is quite difficult to work with. It is poorly documented, the API exposes
low level concepts of the Kafka protocol, and it doesn't support recent Go features
like [contexts](https://golang.org/pkg/context/). It also passes all values as
pointers which causes large numbers of dynamic memory allocations, more frequent
garbage collections, and higher memory usage.

- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) is a
cgo based wrapper around [librdkafka](https://github.com/edenhill/librdkafka),
which means it introduces a dependency to a C library on all Go code that uses
the package. It has much better documentation than sarama but still lacks support
for Go contexts.

- [goka](https://github.com/lovoo/goka) is a more recent Kafka client for Go
which focuses on a specific usage pattern. It provides abstractions for using Kafka
as a message passing bus between services rather than an ordered log of events, but
this is not the typical use case of Kafka for us at Segment. The package also
depends on sarama for all interactions with Kafka.

This is where `kafka-go` comes into play. It provides both low and high level
APIs for interacting with Kafka, mirroring concepts and implementing interfaces of
the Go standard library to make it easy to use and integrate with existing
software.

## Migrating to 0.4

Version 0.4 introduces a few breaking changes to the repository structure which
should have minimal impact on programs and should only manifest at compile time
(the runtime behavior should remain unchanged).

* Programs do not need to import compression packages anymore in order to read
compressed messages from kafka. All compression codecs are supported by default.

* Programs that used the compression codecs directly must be adapted.
Compression codecs are now exposed in the `compress` sub-package.

* The experimental `kafka.Client` API has been updated and slightly modified:
the `kafka.NewClient` function and `kafka.ClientConfig` type were removed.
Programs now configure the client values directly through exported fields.

* The `kafka.(*Client).ConsumerOffsets` method is now deprecated (along with the
`kafka.TopicAndGroup` type, and will be removed when we release version 1.0.
Programs should use the `kafka.(*Client).OffsetFetch` API instead.

With 0.4, we know that we are starting to introduce a bit more complexity in the
code, but the plan is to eventually converge towards a simpler and more effective
API, allowing us to keep up with Kafka's ever growing feature set, and bringing
a more efficient implementation to programs depending on kafka-go.

We truly appreciate everyone's input and contributions, which have made this
project way more than what it was when we started it, and we're looking forward
to receive more feedback on where we should take it.

## Kafka versions

`kafka-go` is currently compatible with Kafka versions from 0.10.1.0 to 2.1.0. While latest versions will be working,
some features available from the Kafka API may not be implemented yet.

## Golang version

`kafka-go` is currently compatible with golang version from 1.12+. To use with older versions of golang use release [v0.2.5](https://github.com/segmentio/kafka-go/releases/tag/v0.2.5).

## Connection [![GoDoc](https://godoc.org/github.com/segmentio/kafka-go?status.svg)](https://godoc.org/github.com/segmentio/kafka-go#Conn)

The `Conn` type is the core of the `kafka-go` package. It wraps around a raw
network connection to expose a low-level API to a Kafka server.

Here are some examples showing typical use of a connection object:
```go
// to produce messages
topic := "my-topic"
partition := 0

conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)

conn.SetWriteDeadline(time.Now().Add(10*time.Second))
conn.WriteMessages(
    kafka.Message{Value: []byte("one!")},
    kafka.Message{Value: []byte("two!")},
    kafka.Message{Value: []byte("three!")},
)

conn.Close()
```
```go
// to consume messages
topic := "my-topic"
partition := 0

conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)

conn.SetReadDeadline(time.Now().Add(10*time.Second))
batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

b := make([]byte, 10e3) // 10KB max per message
for {
    _, err := batch.Read(b)
    if err != nil {
        break
    }
    fmt.Println(string(b))
}

batch.Close()
conn.Close()
```

Because it is low level, the `Conn` type turns out to be a great building block
for higher level abstractions, like the `Reader` for example.

## Reader [![GoDoc](https://godoc.org/github.com/segmentio/kafka-go?status.svg)](https://godoc.org/github.com/segmentio/kafka-go#Reader)

A `Reader` is another concept exposed by the `kafka-go` package, which intends
to make it simpler to implement the typical use case of consuming from a single
topic-partition pair.
A `Reader` also automatically handles reconnections and offset management, and
exposes an API that supports asynchronous cancellations and timeouts using Go
contexts.

```go
// make a new reader that consumes from topic-A, partition 0, at offset 42
r := kafka.NewReader(kafka.ReaderConfig{
    Brokers:   []string{"localhost:9092"},
    Topic:     "topic-A",
    Partition: 0,
    MinBytes:  10e3, // 10KB
    MaxBytes:  10e6, // 10MB
})
r.SetOffset(42)

for {
    m, err := r.ReadMessage(context.Background())
    if err != nil {
        break
    }
    fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
}

r.Close()
```

### Consumer Groups

```kafka-go``` also supports Kafka consumer groups including broker managed offsets.
To enable consumer groups, simply specify the GroupID in the ReaderConfig.

ReadMessage automatically commits offsets when using consumer groups.

```go
// make a new reader that consumes from topic-A
r := kafka.NewReader(kafka.ReaderConfig{
    Brokers:   []string{"localhost:9092"},
    GroupID:   "consumer-group-id",
    Topic:     "topic-A",
    MinBytes:  10e3, // 10KB
    MaxBytes:  10e6, // 10MB
})

for {
    m, err := r.ReadMessage(context.Background())
    if err != nil {
        break
    }
    fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
}

r.Close()
```

There are a number of limitations when using consumer groups:

* ```(*Reader).SetOffset``` will return an error when GroupID is set
* ```(*Reader).Offset``` will always return ```-1``` when GroupID is set
* ```(*Reader).Lag``` will always return ```-1``` when GroupID is set
* ```(*Reader).ReadLag``` will return an error when GroupID is set
* ```(*Reader).Stats``` will return a partition of ```-1``` when GroupID is set

### Explicit Commits

```kafka-go``` also supports explicit commits.  Instead of calling ```ReadMessage```,
call ```FetchMessage``` followed by ```CommitMessages```.

```go
ctx := context.Background()
for {
    m, err := r.FetchMessage(ctx)
    if err != nil {
        break
    }
    fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
    r.CommitMessages(ctx, m)
}
```

### Managing Commits

By default, CommitMessages will synchronously commit offsets to Kafka.  For
improved performance, you can instead periodically commit offsets to Kafka
by setting CommitInterval on the ReaderConfig.


```go
// make a new reader that consumes from topic-A
r := kafka.NewReader(kafka.ReaderConfig{
    Brokers:        []string{"localhost:9092"},
    GroupID:        "consumer-group-id",
    Topic:          "topic-A",
    MinBytes:       10e3, // 10KB
    MaxBytes:       10e6, // 10MB
    CommitInterval: time.Second, // flushes commits to Kafka every second
})
```

## Writer [![GoDoc](https://godoc.org/github.com/segmentio/kafka-go?status.svg)](https://godoc.org/github.com/segmentio/kafka-go#Writer)

To produce messages to Kafka, a program may use the low-level `Conn` API, but
the package also provides a higher level `Writer` type which is more appropriate
to use in most cases as it provides additional features:

- Automatic retries and reconnections on errors.
- Configurable distribution of messages across available partitions.
- Synchronous or asynchronous writes of messages to Kafka.
- Asynchronous cancellation using contexts.
- Flushing of pending messages on close to support graceful shutdowns.

```go
// make a writer that produces to topic-A, using the least-bytes distribution
w := &kafka.Writer{
	Addr:     kafka.TCP("localhost:9092"),
	Topic:   "topic-A",
	Balancer: &kafka.LeastBytes{},
}

w.WriteMessages(context.Background(),
	kafka.Message{
		Key:   []byte("Key-A"),
		Value: []byte("Hello World!"),
	},
	kafka.Message{
		Key:   []byte("Key-B"),
		Value: []byte("One!"),
	},
	kafka.Message{
		Key:   []byte("Key-C"),
		Value: []byte("Two!"),
	},
)

w.Close()
```

**Note:** Even though kafka.Message contain ```Topic``` and ```Partition``` fields, they **MUST NOT** be
set when writing messages.  They are intended for read use only.

### Writing to multiple topics

Each writer is bound to a single topic, to write to multiple topics, a program
must create multiple writers.

We considered making the `kafka.Writer` type interpret the `Topic` field of
`kafka.Message` values, batching messages per partition or topic/partition pairs
does not introduce much more complexity. However, supporting this means we would
also have to report stats broken down by topic. It would also raise the question
of how we would manage configuration specific to each topic (e.g. different
compression algorithms). Overall, the amount of coupling between the various
properties of `kafka.Writer` suggest that we should not support publishing to
multiple topics from a single writer, and instead encourage the program to
create a writer for each topic it needs to publish to.

The split of connection management into the `kafka.Transport` in kafka-go 0.4 has
made writers really cheap to create as they barely manage any state, programs can
construct new writers configured to publish to kafka topics when needed.

Adding new APIs to facilitate the management of writer sets is an option, and we
would welcome contributions in this area.

### Compatibility with other clients

#### Sarama

If you're switching from Sarama and need/want to use the same algorithm for message
partitioning, you can use the ```kafka.Hash``` balancer.  ```kafka.Hash``` routes
messages to the same partitions that Sarama's default partitioner would route to.

```go
w := &kafka.Writer{
	Addr:     kafka.TCP("localhost:9092"),
	Topic:    "topic-A",
	Balancer: &kafka.Hash{},
}
```

#### librdkafka and confluent-kafka-go

Use the ```kafka.CRC32Balancer``` balancer to get the same behaviour as librdkafka's
default ```consistent_random``` partition strategy.

```go
w := &kafka.Writer{
	Addr:     kafka.TCP("localhost:9092"),
	Topic:    "topic-A",
	Balancer: kafka.CRC32Balancer{},
}
```

#### Java

Use the ```kafka.Murmur2Balancer``` balancer to get the same behaviour as the canonical
Java client's default partitioner.  Note: the Java class allows you to directly specify
the partition which is not permitted.

```go
w := &kafka.Writer{
	Addr:     kafka.TCP("localhost:9092"),
	Topic:    "topic-A",
	Balancer: kafka.Murmur2Balancer{},
}
```

### Compression

Compression can be enabled on the `Writer` by setting the `Compression` field:

```go
w := &kafka.Writer{
	Addr:        kafka.TCP("localhost:9092"),
	Topic:       "topic-A",
	Compression: kafka.Snappy,
}
```

The `Reader` will by determine if the consumed messages are compressed by
examining the message attributes.  However, the package(s) for all expected
codecs must be imported so that they get loaded correctly.

_Note: in versions prior to 0.4 programs had to import compression packages to
install codecs and support reading compressed messages from kafka. This is no
longer the case and import of the compression packages are now no-ops._

## TLS Support

For a bare bones Conn type or in the Reader/Writer configs you can specify a dialer option for TLS support. If the TLS field is nil, it will not connect with TLS.

### Connection

```go
dialer := &kafka.Dialer{
    Timeout:   10 * time.Second,
    DualStack: true,
    TLS:       &tls.Config{...tls config...},
}

conn, err := dialer.DialContext(ctx, "tcp", "localhost:9093")
```

### Reader

```go
dialer := &kafka.Dialer{
    Timeout:   10 * time.Second,
    DualStack: true,
    TLS:       &tls.Config{...tls config...},
}

r := kafka.NewReader(kafka.ReaderConfig{
    Brokers:        []string{"localhost:9093"},
    GroupID:        "consumer-group-id",
    Topic:          "topic-A",
    Dialer:         dialer,
})
```
