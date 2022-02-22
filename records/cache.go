package records

import (
	"container/list"
	"context"
	"net"
	"sync"

	"github.com/petar/GoLLRB/llrb"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/fetch"
)

// Cache is an implementation of the kafka.RoundTripper interface which applies
// a caching layer to fetch requests in order to optimize random access to kafka
// partition offsets.
//
// Applications must configure both the cache size limit and the storage backend
// to enable caching; if either of these are missing, the cache simply forwards
// requests to the transport layer.
//
// The following example shows how an application would typically configure a
// cache on Kafka client:
//
//	client := &kafka.Client{
//		Transport: &records.Cache{
//			SizeLimit: 128 * 1024 * 1024,    // 128 MiB
//			Storage:   records.NewStorage(), // in-memory storage
//		},
//	}
//
// Cache instances are safe to use concurrently from multiple goroutines.
type Cache struct {
	// Limit to the cache size (in bytes).
	//
	// If zero, caching is disabled.
	SizeLimit int64

	// The storage instance used as backend for the cache.
	//
	// The storage is the durability layer of the cache, the rest of the cache
	// state is retained in memory
	//
	// If nil, no caching is applied.
	Storage Storage

	// The transport used to send requests to kafka.
	//
	// If nil, kafka.DefaultTransport is used instead.
	Transport kafka.RoundTripper

	once  once
	mutex sync.Mutex
	queue list.List
	index llrb.LLRB
	size  int64

	stats     sync.Mutex
	bytes     int64
	inserts   int64
	lookups   int64
	hits      int64
	evictions int64
	errors    int64
}

// CacheStats contains statistics about cache utilization.
type CacheStats struct {
	SizeLimit int64
	Size      int64
	Inserts   int64
	Lookups   int64
	Hits      int64
	Evictions int64
	Errors    int64
}

// Stats returns statistics on the utilization of c.
func (c *Cache) Stats() (stats CacheStats) {
	stats.SizeLimit = c.SizeLimit
	c.stats.Lock()
	stats.Size = c.bytes
	stats.Inserts = c.inserts
	stats.Lookups = c.lookups
	stats.Hits = c.hits
	stats.Evictions = c.evictions
	stats.Errors = c.errors
	c.stats.Unlock()
	return stats
}

// RoundTrip satisfies the kafka.RoundTripper interface.
func (c *Cache) RoundTrip(ctx context.Context, addr net.Addr, req kafka.Request) (kafka.Response, error) {
	fetchReq, _ := req.(*fetch.Request)
	if fetchReq == nil || c.Storage == nil || c.SizeLimit == 0 {
		return c.roundTrip(ctx, addr, req)
	}

	if err := c.once.do(func() error { return c.init(ctx) }); err != nil {
		return nil, err
	}

	fetchAddr := addr.String()
	fetchRes, missing, err := c.load(ctx, fetchAddr, fetchReq)
	if err != nil {
		return nil, err
	}

	if missing > 0 {
		fetchMissingReq, mapping := fetchRequestOfMissingOffsets(fetchReq, fetchRes, missing)

		r, err := c.roundTrip(ctx, addr, fetchMissingReq)
		if err != nil {
			return nil, err
		}

		f := r.(*fetch.Response)
		if err := c.store(ctx, fetchAddr, f); err != nil {
			closeRecords(fetchRes)
			closeRecords(f)
			return nil, err
		}

		mappingIndex := 0
		for _, t := range f.Topics {
			for _, p := range t.Partitions {
				m := mapping[mappingIndex]
				mappingIndex++
				fetchRes.Topics[m.topic].Partitions[m.partition] = p
			}
		}
	}

	return fetchRes, nil
}

func (c *Cache) roundTrip(ctx context.Context, addr net.Addr, req kafka.Request) (kafka.Response, error) {
	return c.transport().RoundTrip(ctx, addr, req)
}

func (c *Cache) transport() kafka.RoundTripper {
	if c.Transport != nil {
		return c.Transport
	}
	return kafka.DefaultTransport
}

func (c *Cache) init(ctx context.Context) error {
	evictions, errors, size := 0, 0, int64(0)
	defer func() {
		c.stats.Lock()
		c.bytes = size
		c.evictions += int64(evictions)
		c.errors += int64(errors)
		c.stats.Unlock()
	}()

	it := c.Storage.List(ctx)
	defer it.Close()

	for it.Next() {
		evictCount, sizeDelta, err := c.add(ctx, it.Key(), it.Size())
		evictions += evictCount
		size += sizeDelta
		if err != nil {
			c.reset()
			size = 0
			errors++
			return err
		}
	}

	return it.Close()
}

func (c *Cache) reset() {
	c.mutex.Lock()
	c.index = llrb.LLRB{}
	c.queue = list.List{}
	c.size = 0
	c.mutex.Unlock()
}

func (c *Cache) load(ctx context.Context, addr string, req *fetch.Request) (*fetch.Response, int, error) {
	key := &cacheKey{Key: Key{Addr: addr}}
	res := &fetch.Response{Topics: make([]fetch.ResponseTopic, len(req.Topics))}
	missing := 0
	lookups := 0
	errors := 0

	defer func() {
		c.stats.Lock()
		c.lookups += int64(lookups)
		c.hits += int64(lookups - missing)
		c.errors += int64(errors)
		c.stats.Unlock()
	}()

	for i, t := range req.Topics {
		key.Topic = t.Topic
		resTopic := &res.Topics[i]
		resTopic.Partitions = make([]fetch.ResponsePartition, len(t.Partitions))

		for j, p := range t.Partitions {
			resTopic.Partitions[j] = fetch.ResponsePartition{
				Partition: p.Partition,
				// We cannot guess the high watermark nor other offsets when
				// returning cached records.
				HighWatermark:    -1,
				LastStableOffset: -1,
				LogStartOffset:   -1,
				RecordSet: protocol.RecordSet{
					Version: 2,
				},
			}
		}

		for j, p := range t.Partitions {
			key.Partition = int(p.Partition)
			key.BaseOffset = p.FetchOffset
			lookups++

			e := c.lookup(key)
			if e == nil {
				missing++
				continue
			}

			v, err := c.Storage.Load(ctx, e.Key)
			if err != nil {
				if err == ErrNotFound {
					missing++
					continue
				}
				errors++
				closeRecords(res)
				return nil, 0, err
			}

			_, err = resTopic.Partitions[j].RecordSet.ReadFrom(v)
			v.Close()
			if err != nil {
				errors++
				closeRecords(res)
				return nil, 0, err
			}
		}
	}

	return res, missing, nil
}

func (c *Cache) store(ctx context.Context, addr string, res *fetch.Response) error {
	inserts, evictions, errors, sizeDelta := 0, 0, 0, int64(0)
	defer func() {
		c.stats.Lock()
		c.bytes += sizeDelta
		c.inserts += int64(inserts)
		c.evictions += int64(evictions)
		c.errors += int64(errors)
		c.stats.Unlock()
	}()

	for _, t := range res.Topics {
		for i := range t.Partitions {
			insertCount, evictCount, sizeDiff, err := c.storeTopicPartitionRecords(ctx, addr, t.Topic, &t.Partitions[i])
			inserts += insertCount
			evictions += evictCount
			sizeDelta += sizeDiff
			if err != nil {
				errors++
				return err
			}
		}
	}

	return nil
}

func (c *Cache) storeTopicPartitionRecords(ctx context.Context, addr, topic string, partition *fetch.ResponsePartition) (inserts, evictions int, sizeDelta int64, err error) {
	// TODO: should we cache control batches?
	switch r := partition.RecordSet.Records.(type) {
	case *protocol.RecordBatch:
		inserts = 1
		evictions, sizeDelta, err = c.storeRecordBatch(ctx, addr, topic, partition, r)
	case *protocol.RecordStream:
		for _, records := range r.Records {
			if recordBatch, _ := records.(*protocol.RecordBatch); recordBatch != nil {
				inserts++
				evictCount, sizeDiff, err := c.storeRecordBatch(ctx, addr, topic, partition, recordBatch)
				evictions += evictCount
				sizeDelta += sizeDiff
				if err != nil {
					return inserts, evictions, sizeDelta, err
				}
			}
		}
	}
	return inserts, evictions, sizeDelta, nil
}

func (c *Cache) storeRecordBatch(ctx context.Context, addr, topic string, partition *fetch.ResponsePartition, batch *protocol.RecordBatch) (evictions int, sizeDelta int64, err error) {
	defer batch.Records.Close()
	key := Key{
		Addr:       addr,
		Topic:      topic,
		Partition:  int(partition.Partition),
		BaseOffset: batch.BaseOffset,
	}

	n, err := c.Storage.Store(ctx, key, batch)
	if err != nil {
		return evictions, sizeDelta, err
	}

	evictCount, sizeDiff, err := c.add(ctx, key, n)
	evictions += evictCount
	sizeDelta += sizeDiff
	if err != nil {
		return evictions, sizeDelta, err
	}

	// TODO: we should improve the cache synchronization strategy here to handle
	// the very unlikely scenario where loading the key that we just stored
	// would fail due to concurrent evictions.
	r, err := c.Storage.Load(ctx, key)
	if err != nil {
		return evictions, sizeDelta, err
	}
	defer r.Close()
	_, err = batch.ReadFrom(r)
	return evictions, sizeDelta, err
}

func (c *Cache) lookup(k *cacheKey) *cacheEntry {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	e, _ := c.index.Get(k).(*cacheEntry)
	if e != nil {
		c.queue.MoveToFront(e.hook)
	}

	return e
}

func (c *Cache) add(ctx context.Context, key Key, size int64) (evictions int, sizeDelta int64, err error) {
	sizeDelta += size

	if evicted, sizeDrop := c.insert(key, size); len(evicted) != 0 {
		evictions += len(evicted)
		sizeDelta -= sizeDrop

		if err := c.Storage.Delete(ctx, evicted); err != nil {
			return evictions, sizeDelta, err
		}
	}

	return 0, 0, nil
}

func (c *Cache) insert(key Key, size int64) (evicted []Key, sizeDrop int64) {
	e := &cacheEntry{Key: key, size: size}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.index.InsertNoReplace(e)
	e.hook = c.queue.PushFront(e)
	c.size += e.size

	for c.queue.Len() > 1 && c.size > c.SizeLimit {
		x := c.queue.Back().Value.(*cacheEntry)
		c.index.Delete(x)
		c.queue.Remove(x.hook)
		c.size -= x.size
		sizeDrop += x.size
		evicted = append(evicted, x.Key)
	}

	return evicted, sizeDrop
}

type topicPartitionIndex struct {
	topic     int32
	partition int32
}

func fetchRequestOfMissingOffsets(req *fetch.Request, res *fetch.Response, missing int) (*fetch.Request, []topicPartitionIndex) {
	mapping := make([]topicPartitionIndex, 0, missing)
	fetchReq := *req
	fetchReq.Topics = make([]fetch.RequestTopic, 0, missing)

	for i, t := range res.Topics {
		fetchTopic := fetch.RequestTopic{
			Topic: t.Topic,
		}

		for j, p := range t.Partitions {
			if p.RecordSet.Records == nil {
				fetchTopic.Partitions = append(fetchTopic.Partitions, req.Topics[i].Partitions[j])

				mapping = append(mapping, topicPartitionIndex{
					topic:     int32(i),
					partition: int32(j),
				})
			}
		}

		if len(fetchTopic.Partitions) > 0 {
			fetchReq.Topics = append(fetchReq.Topics, fetchTopic)
		}
	}

	return &fetchReq, mapping
}

func closeRecords(r *fetch.Response) {
	for _, t := range r.Topics {
		for _, p := range t.Partitions {
			p.RecordSet.Records.Close()
		}
	}
}

type cacheKey struct{ Key }

func (k *cacheKey) Less(item llrb.Item) bool {
	return k.Key.Less(&item.(*cacheEntry).Key)
}

type cacheEntry struct {
	Key
	hook *list.Element
	size int64
}

func (e *cacheEntry) Less(item llrb.Item) bool {
	switch x := item.(type) {
	case *cacheKey:
		return e.Key.Less(&x.Key)
	case *cacheEntry:
		return e.Key.Less(&x.Key)
	default:
		panic("BUG: cannot compare cache entry with item that is neither a *cacheEntry nor a *fetchRequestKey")
	}
}
