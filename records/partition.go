package records

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/datastructures/v2/cache"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/fetch"
	"github.com/segmentio/kafka-go/protocol/produce"
)

type Partition struct {
	Addr        net.Addr
	Topic       string
	Partition   int
	Transport   kafka.RoundTripper
	Compression kafka.Compression

	ValueCacheSize  int64
	RecordCacheSize int64

	Index   Index
	Storage Storage

	addr    string
	cancel  context.CancelFunc
	closed  bool
	sync    sync.RWMutex
	wait    sync.WaitGroup
	pool    sync.Pool
	once    sync.Once
	mutex   sync.Mutex
	records cache.LRU[string, *record]
	size    int64
	kafka   Cache
	offsets barrier
}

type partitionError struct {
	par *Partition
	err error
}

func (pe *partitionError) Error() string {
	return fmt.Sprintf("%s/%s/%d: %v", pe.par.Addr, pe.par.Topic, pe.par.Partition, pe.err)
}

func (pe *partitionError) Unwrap() error {
	return pe.err
}

func (p *Partition) errorf(msg string, args ...interface{}) error {
	return p.wrapError(fmt.Errorf(msg, args...))
}

func (p *Partition) wrapError(err error) error {
	return &partitionError{par: p, err: err}
}

func (p *Partition) Close() error {
	p.sync.Lock()
	p.closed = true
	p.sync.Unlock()

	p.once.Do(func() {})
	if p.cancel != nil {
		p.cancel()
	}

	p.wait.Wait()
	return nil
}

func (p *Partition) Insert(ctx context.Context, records kafka.RecordReader) error {
	defer records.Close()
	p.once.Do(p.init)
	p.sync.RLock()
	defer p.sync.RUnlock()

	if p.closed {
		return ErrClosed
	}

	_, _, lastOffset, err := p.insertRecordsInKafka(ctx, records)
	if err != nil {
		return err
	}

	p.offsets.observeLastOffset(lastOffset)
	return nil
}

func (p *Partition) Lookup(ctx context.Context, key []byte) (Value, error) {
	p.once.Do(p.init)
	p.sync.RLock()
	defer p.sync.RUnlock()

	if p.closed {
		return nil, ErrClosed
	}

	if err := p.offsets.sync(ctx); err != nil {
		return nil, p.errorf("waiting on last offset to stabilize: %w", err)
	}

	rec := p.lookupKeyInCache(key)
	if rec == nil {
		offset, err := p.lookupOffsetInIndex(ctx, key)
		if err != nil {
			return nil, err
		}
		records, err := p.lookupRecordsInKafka(ctx, offset)
		if err != nil {
			return nil, err
		}
		if err := p.update(ctx, records, func(r *record) error {
			if r.offset == offset {
				rec = r
				r.ref()
			}
			return nil
		}); err != nil {
			return nil, err
		}
		if rec == nil {
			return nil, ErrNotFound
		}
	}

	return &value{rec: rec, pool: &p.pool}, nil
}

func (p *Partition) readRecord(k *kafka.Record) (*record, error) {
	r, _ := p.pool.Get().(*record)
	if r == nil {
		r = &record{refc: 1}
	} else {
		r.ref()
	}
	if err := r.readFrom(k); err != nil {
		r.unref()
		return nil, fmt.Errorf("record at offset %d: %w", k.Offset, err)
	}
	return r, nil
}

func (p *Partition) update(ctx context.Context, records kafka.RecordReader, do func(*record) error) error {
	update := p.Index.Update(ctx)
	defer update.Close()

	if err := forEachRecord(records, func(k *kafka.Record) error {
		r, err := p.readRecord(k)
		if err != nil {
			return err
		}
		defer r.unref()

		key := r.key.Bytes()
		if k.Value == nil {
			// null keys in kafka messages indicate records that should be
			// removed from the partition.
			err = update.Delete(key)
		} else {
			err = update.Insert(key, r.offset)
		}
		if err != nil {
			return fmt.Errorf("key %q at offset %d: %w", key, r.offset, err)
		}

		if sizeLimit := p.ValueCacheSize; sizeLimit > 0 {
			p.insertRecordInCache(r, sizeLimit)
		}
		return do(r)
	}); err != nil {
		return p.errorf("updating record index: %w", err)
	}

	if err := update.Commit(); err != nil {
		return p.errorf("committing record index updates: %w", err)
	}
	return nil
}

func (p *Partition) insertRecordInCache(r *record, sizeLimit int64) {
	cacheKey := r.key.String()
	evictedRecords := make([]*record, 0, 8)

	r.ref()
	p.mutex.Lock()
	previous, _ := p.records.Insert(cacheKey, r)
	p.size += r.size()
	if previous != nil {
		p.size -= previous.size()
	}
	for p.size > sizeLimit {
		_, evicted, _ := p.records.Evict()
		evictedRecords = append(evictedRecords, evicted)
		p.size -= evicted.size()
	}
	p.mutex.Unlock()

	if previous != nil {
		previous.unref()
	}

	for _, evicted := range evictedRecords {
		evicted.unref()
	}
}

func (p *Partition) insertRecordsInKafka(ctx context.Context, records kafka.RecordReader) (firstOffset, baseOffset, lastOffset int64, err error) {
	numRecords := records.Len()

	r, err := p.kafka.RoundTrip(ctx, p.Addr, &produce.Request{
		Acks: int16(kafka.RequireAll),
		Topics: []produce.RequestTopic{{
			Topic: p.Topic,
			Partitions: []produce.RequestPartition{{
				Partition: int32(p.Partition),
				RecordSet: protocol.RecordSet{
					Attributes: protocol.Attributes(p.Compression),
					Records:    records,
				},
			}},
		}},
	})
	if err != nil {
		return -1, -1, -1, err
	}

	res := r.(*produce.Response)
	firstOffset, baseOffset, lastOffset = -1, -1, -1

	for _, topic := range res.Topics {
		if topic.Topic == p.Topic {
			for _, partition := range topic.Partitions {
				if partition.Partition == int32(p.Partition) {
					if partition.ErrorCode != 0 {
						err = p.errorf("%w: %s", kafka.Error(partition.ErrorCode), partition.ErrorMessage)
						return
					}
					for _, re := range partition.RecordErrors {
						err = p.errorf("writing record %d/%d: %s", re.BatchIndex, numRecords, re.BatchIndexErrorMessage)
						return
					}
					firstOffset = partition.LogStartOffset
					baseOffset = partition.BaseOffset
					lastOffset = partition.BaseOffset + int64(numRecords)
					return
				}
			}
		}
	}

	err = p.errorf("topic or partition not found in kafka response: %+v", res)
	return
}

func (p *Partition) lookupKeyInCache(key []byte) *record {
	cacheKey := string(key)
	p.mutex.Lock()
	r, _ := p.records.Lookup(cacheKey)
	if r != nil {
		r.ref()
	}
	p.mutex.Unlock()
	return r
}

func (p *Partition) lookupOffsetInIndex(ctx context.Context, key []byte) (offset int64, err error) {
	s := p.Index.Select(ctx, key)
	if s.Next() {
		k, x := s.Entry()
		if bytes.Equal(k, key) {
			offset = x
		} else {
			return -1, ErrNotFound
		}
	}
	if err := s.Close(); err != nil {
		return -1, p.errorf("looking up offset %d in index: %w", err)
	}
	return offset, nil
}

func (p *Partition) lookupRecordsInKafka(ctx context.Context, offset int64) (kafka.RecordReader, error) {
	maxBytes := int32(8 * 1024 * 1024)

	r, err := p.kafka.RoundTrip(ctx, p.Addr, &fetch.Request{
		ReplicaID:      -1,
		MaxWaitTime:    0,
		MinBytes:       0,
		MaxBytes:       maxBytes,
		IsolationLevel: int8(kafka.ReadCommitted),
		SessionID:      -1,
		SessionEpoch:   -1,
		Topics: []fetch.RequestTopic{{
			Topic: p.Topic,
			Partitions: []fetch.RequestPartition{{
				Partition:          int32(p.Partition),
				CurrentLeaderEpoch: -1,
				FetchOffset:        offset,
				LogStartOffset:     -1,
				PartitionMaxBytes:  maxBytes,
			}},
		}},
	})
	if err != nil {
		return nil, err
	}
	res := r.(*fetch.Response)
	_ = res //
	return nil, nil
}

func (p *Partition) init() {
	ctx, cancel := context.WithCancel(context.Background())
	p.addr = p.Addr.String()
	p.cancel = cancel
	p.kafka.SizeLimit = p.RecordCacheSize
	p.kafka.Storage = p.Storage
	p.kafka.Transport = p.Transport
	p.wait.Add(1)
	go p.fetchLoop(ctx)
}

func (p *Partition) fetchLoop(ctx context.Context) {
	for ctx.Err() == nil {
		if err := p.fetch(ctx); err != nil {
			// TODO: log?
		}
	}
}

func (p *Partition) fetch(ctx context.Context) error {
	const maxWait = 500 * time.Millisecond
	const timeout = 4 * maxWait

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return nil // TODO
}

func forEachRecord(records kafka.RecordReader, do func(*kafka.Record) error) error {
	defer records.Close()
	for {
		r, err := records.ReadRecord()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
		if err := do(r); err != nil {
			return err
		}
	}
}

type record struct {
	refc   uintptr
	key    bytes.Buffer
	value  bytes.Buffer
	offset int64
}

func (r *record) size() int64 {
	return int64(r.key.Len()) + int64(r.value.Len())
}

func (r *record) reset() {
	r.key.Reset()
	r.value.Reset()
	r.offset = 0
}

func (r *record) ref() {
	atomic.AddUintptr(&r.refc, +1)
}

func (r *record) unref() uintptr {
	return atomic.AddUintptr(&r.refc, ^uintptr(0))
}

func (r *record) readFrom(k *kafka.Record) error {
	if k.Key != nil {
		if _, err := r.key.ReadFrom(k.Key); err != nil {
			return err
		}
	}
	if k.Value != nil {
		if _, err := r.value.ReadFrom(k.Value); err != nil {
			return err
		}
	}
	r.offset = k.Offset
	return nil
}

type Value interface {
	io.Closer
	io.ReaderAt
	Size() int64
}

type value struct {
	rec  *record
	pool *sync.Pool
}

func (v *value) Size() int64 {
	if v.rec != nil {
		return int64(v.rec.value.Len())
	}
	return 0
}

func (v *value) Close() error {
	if v.rec != nil {
		if v.rec.unref() == 0 {
			v.rec.reset()
			v.pool.Put(v.rec)
		}
		v.rec = nil
	}
	return nil
}

func (v *value) ReadAt(b []byte, off int64) (int, error) {
	if v.rec != nil {
		if data := v.rec.value.Bytes(); off < int64(len(data)) {
			return copy(b, data[off:]), nil
		}
	}
	return 0, io.EOF
}
