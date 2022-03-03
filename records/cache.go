package records

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/segmentio/datastructures/v2/container/list"
	"github.com/segmentio/datastructures/v2/container/tree"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/fetch"
)

// Key is a comparable type representing the identifiers that record batches
// can be indexed by in a storage instance.
type Key struct {
	Addr            string
	Topic           string
	Partition       int32
	LastOffsetDelta int32
	BaseOffset      int64
}

// String returns a human-readable representation of k.
func (k Key) String() string {
	if k == (Key{}) {
		return "(no key)"
	}
	return fmt.Sprintf("%s/%s.%d[%d:%d]", k.Addr, k.Topic, k.Partition, k.BaseOffset, k.endOffset())
}

func (k1 Key) compare(k2 Key) int {
	if cmp := strings.Compare(k1.Addr, k2.Addr); cmp != 0 {
		return cmp
	}
	if cmp := strings.Compare(k1.Topic, k2.Topic); cmp != 0 {
		return cmp
	}
	if cmp := k1.Partition - k2.Partition; cmp != 0 {
		return int(cmp)
	}
	return int(k1.BaseOffset - k2.BaseOffset)
}

func (k1 *Key) match(k2 *Key) bool {
	return k1.Addr == k2.Addr && k1.Topic == k2.Topic && k1.Partition == k2.Partition
}

func (k1 *Key) contains(k2 *Key) bool {
	return k1.match(k2) && k1.containsOffset(k2.BaseOffset)
}

func (k *Key) containsOffset(offset int64) bool {
	return (k.BaseOffset == offset || (k.BaseOffset < offset && offset < k.endOffset()))
}

func (k *Key) endOffset() int64 {
	return k.BaseOffset + int64(k.LastOffsetDelta)
}

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
	index tree.Map[Key, *list.Element[cacheEntry]]
	queue list.List[cacheEntry]
	bytes int64

	stats     sync.Mutex
	size      int64
	keys      int64
	records   int64
	inserts   int64
	lookups   int64
	hits      int64
	evictions int64
	errors    int64
}

type cacheEntry struct {
	key  Key
	size int64
}

// CacheStats contains statistics about cache utilization.
type CacheStats struct {
	SizeLimit int64
	Size      int64
	Keys      int64
	Records   int64
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
	stats.Size = c.size
	stats.Keys = c.keys
	stats.Records = c.records
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
	c.index.Init(Key.compare)

	evictions, errors, size := 0, 0, int64(0)
	defer func() {
		c.stats.Lock()
		c.size = size
		c.evictions += int64(evictions)
		c.errors += int64(errors)
		c.stats.Unlock()
	}()

	it := c.Storage.List(ctx)
	defer it.Close()

	for it.Next() {
		_, evictCount, sizeDelta, err := c.add(ctx, it.Key(), it.Size())
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
	c.index.Init(Key.compare)
	c.queue = list.List[cacheEntry]{}
	c.bytes = 0
	c.mutex.Unlock()
}

func (c *Cache) load(ctx context.Context, addr string, req *fetch.Request) (*fetch.Response, int, error) {
	key := Key{Addr: addr}
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
		resTopic.Topic = t.Topic
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
			key.Partition = p.Partition
			key.BaseOffset = p.FetchOffset
			lookups++

			k, ok := c.lookup(key)
			if !ok {
				missing++
				continue
			}

			v, err := c.Storage.Load(ctx, k)
			if err != nil {
				if err == ErrNotFound {
					missing++
					continue
				}
				errors++
				closeRecords(res)
				return nil, 0, err
			}

			recordBatch := new(protocol.RecordBatch)
			_, err = recordBatch.ReadFrom(v)
			v.Close()
			if err != nil {
				errors++
				closeRecords(res)
				return nil, 0, err
			}

			recordSet := &resTopic.Partitions[j].RecordSet
			recordSet.Version = 2
			recordSet.Attributes = recordBatch.Attributes
			recordSet.Records = recordBatch
		}
	}

	return res, missing, nil
}

func (c *Cache) store(ctx context.Context, addr string, res *fetch.Response) error {
	inserts, evictions, errors, sizeDelta := 0, 0, 0, int64(0)
	defer func() {
		c.stats.Lock()
		c.size += sizeDelta
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
	var inserted bool
	// TODO: should we cache control batches?
	switch r := partition.RecordSet.Records.(type) {
	case *protocol.RecordBatch:
		inserted, evictions, sizeDelta, err = c.storeRecordBatch(ctx, addr, topic, partition, r)
		if inserted {
			inserts = 1
		}
	case *protocol.RecordStream:
		for _, records := range r.Records {
			if recordBatch, _ := records.(*protocol.RecordBatch); recordBatch != nil {
				inserted, evictCount, sizeDiff, err := c.storeRecordBatch(ctx, addr, topic, partition, recordBatch)
				evictions += evictCount
				sizeDelta += sizeDiff
				if inserted {
					inserts++
				}
				if err != nil {
					return inserts, evictions, sizeDelta, err
				}
			}
		}
	}
	return inserts, evictions, sizeDelta, nil
}

func (c *Cache) storeRecordBatch(ctx context.Context, addr, topic string, partition *fetch.ResponsePartition, batch *protocol.RecordBatch) (inserted bool, evictions int, sizeDelta int64, err error) {
	defer batch.Records.Close()

	key := Key{
		Addr:            addr,
		Topic:           topic,
		Partition:       partition.Partition,
		LastOffsetDelta: batch.LastOffsetDelta,
		BaseOffset:      batch.BaseOffset,
	}

	// protocol.RecordBatch implements the io.WriterTo interface, which makes it
	// possible to store the batch directly into the store. If the underlying
	// type of the batch's Record field allows it, records data will be copied
	// unchanged to the store without being re-encoded.
	n, err := c.Storage.Store(ctx, key, batch)
	if err != nil {
		return false, evictions, sizeDelta, err
	}

	inserted, evictCount, sizeDiff, err := c.add(ctx, key, n)
	evictions += evictCount
	sizeDelta += sizeDiff
	if err != nil {
		return inserted, evictions, sizeDelta, err
	}

	// TODO: we should improve the cache synchronization strategy here to handle
	// the very unlikely scenario where loading the key that we just stored
	// would fail due to concurrent evictions.
	r, err := c.Storage.Load(ctx, key)
	if err != nil {
		return inserted, evictions, sizeDelta, err
	}
	defer r.Close()
	_, err = batch.ReadFrom(r)
	return inserted, evictions, sizeDelta, err
}

func (c *Cache) lookup(key Key) (Key, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	k, e, ok := c.index.Search(key)
	if ok && k.contains(&key) {
		c.queue.MoveToFront(e)
		return k, true
	}

	return Key{}, false
}

func (c *Cache) add(ctx context.Context, key Key, size int64) (inserted bool, evictions int, sizeDelta int64, err error) {
	sizeDelta += size

	inserted, evicted, sizeDrop := c.insert(key, size)
	if len(evicted) != 0 {
		evictions += len(evicted)
		sizeDelta -= sizeDrop

		if err := c.Storage.Delete(ctx, evicted); err != nil {
			return inserted, evictions, sizeDelta, err
		}
	}

	return inserted, evictions, sizeDelta, nil
}

func (c *Cache) insert(key Key, size int64) (inserted bool, evicted []Key, sizeDrop int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.index.Lookup(key); exists {
		return false, nil, 0
	}

	c.index.Insert(key, c.queue.PushFront(cacheEntry{
		key:  key,
		size: size,
	}))
	c.bytes += size

	for c.queue.Len() > 1 && c.bytes > c.SizeLimit {
		e := c.queue.Back()
		c.queue.Remove(e)
		evicted = append(evicted, e.Value.key)
		sizeDrop += e.Value.size
		c.bytes -= e.Value.size
	}

	return true, evicted, sizeDrop
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
