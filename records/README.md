# kafka-go/records

The `github.com/segmentio/kafka-go/records` package provides higher level
components for building advanced Kafka client applications.

## Caching Records: [records.Cache](https://pkg.go.dev/github.com/segmentio/kafka-go/records#Cache)

Applications using Kafka typically consume partitions sequentially from the
first to the last offsets, in which case the data is always new. In some cases,
clients might be designed to leverage Kafka as more than a pub/sub system, using
the ability to reference records by offset as a general purpose storage solution.

In such cases, random access patterns are common and the cost of retrieving
offsets from arbitrary positions within Kafka partitions can be high: fetching
records over the network introduces latency and read amplification (due to Kafka
always returning batches of compressed records), and the requested partition
ranges are likely to have been paged out which increases I/O pressure on the
Kafka brokers.

For high throughput systems, those constraints result in impractical performance
degradations. The use of a local cache is an effective solution to reduce the
Kafka broker access frequency.

The `records.Cache` type is the implementation of a `kafka.RoundTripper` which
caches responses to fetch requests in order to optimize random access patterns
into Kafka partitions. The following example demonstrates how an application
would use an in-memory cache to retain records fetched from Kafka:

```go
client := &kafka.Client{
    Transport: &records.Cache{
        SizeLimit: 256 * 1024 * 1024, // 256MiB
        Storage:   records.NewStorage(),
    },
}

r, err := client.Fetch(ctx, &kafka.FetchRequest{
    ...
})
```

The caching and storage logic are decoupled through the use of the
`records.Storage` interface. The package provides general purpose
implementations of the interface that should suit the needs of most
applications. Systems with more specialized requirements may provide their own
implementation of the interface to customize the storage layer while still being
able to leverage the `records.Cache` type.

## Storing Cached Records on Disk: [records.MountPoint](https://pkg.go.dev/github.com/segmentio/kafka-go/records#MountPoint)

While tracking records in memory enables very fast lookups, having the cache
state expire when the application terminates is not always a desirable behavior
as it often means that a lot of state needs to be fetched back form the Kafka
brokers on start; all fetch requests that would have hit the cache prior to
restarting the application need to retrieve data back from Kafka partitions.

The `records.MountPoint` type provides an implementation of the
`records.Storage` interface which uses the local file system to store and index
records, allowing the cache state to persist across restarts of an application:

```go
storage, err := records.Mount("/mnt/cache")
if err != nil {
    ...
}

cache := &records.Cache{
    SizeLimit: 64 * 1024 * 1024 * 1024, // 64GiB
    Storage:   storage,
}
...
```

Record batches are stored as they were returned by Kafka in response to the
fetch requests. Besides being compute, space and bandwidth efficient (leveraging
Kafka's end-to-end compression capabilities), it also helps with planning for
storage capacity of the caching layer since the size on disk is directly derived
from the size of the Kafka partitions being cached.

For a partition of size _P_ with a replication factor _R_, the size _C_ of a
cache that would hold the entire partition is _C = P / R_.
