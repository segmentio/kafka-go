package kafka

import (
	"context"
	"strconv"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
)

const baseKey = "group-readers/"

type consulReader struct {
	reader  Reader
	client  *api.Client
	id      int
	name    string
	session string
}

type GroupConfig struct {
	Name    string
	Addr    string
	Brokers []string
	Topic   string
	// Number of partitions
	Partitions int

	RequestMaxWaitTime time.Duration
	RequestMinBytes    int
	RequestMaxBytes    int
}

func NewGroupReader(config GroupConfig) (Reader, error) {
	conf := api.DefaultConfig()
	conf.Address = config.Addr

	client, err := api.NewClient(conf)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create consul client")
	}

	entry := api.SessionEntry{
		Name:     "group reader: " + config.Name,
		Behavior: "delete",
	}

	sessionID, _, err := client.Session().Create(&entry, &api.WriteOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create consul session")
	}

	consulReader := &consulReader{
		name:    config.Name,
		client:  client,
		session: sessionID,
	}

	partitionID, err := consulReader.acquire(config.Partitions)
	if err != nil {
		return nil, err
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

func (reader *consulReader) acquire(partitions int) (int, error) {
	// Cycle through each partition and try to acquire a lock to own it.
	// Consul doesn't have support for sequential keys.
	for partition := 0; partition < partitions; partition++ {
		pair := &api.KVPair{
			Key:     baseKey + reader.name + "/" + strconv.Itoa(partition),
			Value:   []byte{},
			Session: reader.session,
		}

		res, _, err := reader.client.KV().Acquire(pair, &api.WriteOptions{})
		if err != nil {
			return 0, err
		}

		if res {
			return partition, nil
		}
	}

	return 0, errors.New("failed to acquire consul lock")
}

func (reader *consulReader) Read(ctx context.Context) (Message, error) {
	return reader.reader.Read(ctx)
}

func (reader *consulReader) Offset() int64 {
	return reader.reader.Offset()
}

func (reader *consulReader) Seek(ctx context.Context, offset int64) (int64, error) {
	return reader.reader.Seek(ctx, offset)
}

func (reader *consulReader) Close() error {
	reader.client.Session().Destroy(reader.session, &api.WriteOptions{})
	return reader.reader.Close()
}
