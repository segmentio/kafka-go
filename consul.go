package kafka

import (
	"context"
	"strconv"
	"time"

	"github.com/pkg/errors"
	consul "github.com/segmentio/consul-go"
)

const baseKey = "group-readers/"

// Consul reader implements consumer groups via Consul locks to ensure
// that each reader can acquire an exclusive partition. This doesn't spread partitions
// among readers but simply assigns partitions to readers. If there are 100 partitions then
// there needs to be 100 readers.
//
// The only reason this needs to implement the `Reader` interface itself is because
// the session needs to be terminated so that the acquired locks are released.
type consulReader struct {
	// The underlying reader
	reader Reader
	// Consul client
	client consul.Client
	// The name of the group. Allows multiple groups without clashing.
	name   string
	cancel context.CancelFunc
}

type GroupConfig struct {
	// The group name
	Name string
	// The Consul address. e.g., http://localhost:8500
	Addr string
	// List of Kafka brokers.
	Brokers []string
	// The Kafka topic the readers should consume from.
	Topic string
	// Number of partitions the topic has. This can be lower than the true number of partitions
	// but cannot be higher.
	Partitions int

	// Request-level settings.
	RequestMaxWaitTime time.Duration
	RequestMinBytes    int
	RequestMaxBytes    int
}

// Create a new Consul-based consumer group returning a Kafka reader that has acquired
// a partition. To read from all partitions you must call `NewGroupReader` N times for N partitions.
// Each reader will acquire an exclusive partition.
func NewGroupReader(ctx context.Context, config GroupConfig) (Reader, error) {
	client := consul.Client{
		Address: config.Addr,
	}

	session := consul.Session{
		Client:   &client,
		Name:     config.Name,
		Behavior: consul.Delete,
	}

	ctx, cancel := consul.WithSession(ctx, session)

	consulReader := &consulReader{
		cancel: cancel,
		client: client,
	}

	partitionID, err := consulReader.acquire(ctx, config.Partitions)
	if err != nil {
		return nil, errors.Wrap(err, "failed to acquire an exclusive lock on a partition")
	}

	consulReader.reader, err = NewReader(ReaderConfig{
		Brokers:   config.Brokers,
		Topic:     config.Topic,
		Partition: partitionID,

		RequestMaxWaitTime: config.RequestMaxWaitTime,
		RequestMinBytes:    config.RequestMinBytes,
		RequestMaxBytes:    config.RequestMaxBytes,
	})

	if err != nil {
		return nil, err
	}

	return consulReader, nil
}

func (reader *consulReader) acquire(ctx context.Context, partitions int) (int, error) {
	// Cycle through each partition and try to acquire a lock to own it.
	// Consul doesn't have support for sequential keys.
	for partition := 0; partition < partitions; partition++ {
		lock, _ := consul.Lock(ctx, baseKey+reader.name+"/"+strconv.Itoa(partition))
		if lock.Err() != nil {
			continue
		}

		return partition, nil
	}

	return 0, errors.New("failed to acquire consul lock")
}

func (reader *consulReader) Lag() int64 {
	return reader.reader.Lag()
}

// Read the next message from the underlying reader
func (reader *consulReader) Read(ctx context.Context) (Message, error) {
	return reader.reader.Read(ctx)
}

// Return the current offset from the underlying reader
func (reader *consulReader) Offset() int64 {
	return reader.reader.Offset()
}

// Seek the underlying reader to a new offset
func (reader *consulReader) Seek(ctx context.Context, offset int64) (int64, error) {
	return reader.reader.Seek(ctx, offset)
}

// Release any locks/keys that were acquired and close the underlying reader.
func (reader *consulReader) Close() error {
	reader.cancel()
	return reader.reader.Close()
}
