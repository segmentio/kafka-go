package kafka

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Offset is a pair of an absolution position in a Kafka partition and a
// timestamp.
type Offset struct {
	Value int64
	Time  time.Time
}

// String satisfies the fmt.Stringer interface.
func (off Offset) String() string {
	return fmt.Sprintf("{ offset:%d, time:%s }", off.Value, off.Time.Format(time.RFC3339))
}

// OffsetStore is an interface implemented by types that support storing and
// retrieving sequences of offsets.
//
// Implementations of the OffsetStore interface must be safe to use concurrently
// from multiple goroutines.
type OffsetStore interface {
	// Returns an iterator over the sequence of offsets in the store.
	ReadOffsets() OffsetIter

	// Write a sequence of offsets to the store, or return an error if storing
	// the offsets failed. The operation must be atomic, offsets must either be
	// fully written or not, it cannot partially succeed. The operation must
	// also be idempotent, it cannot return an error if some offsets already
	// existed in the store.
	WriteOffsets(offsets ...Offset) error

	// Delete a sequence of offsets from the store, or return an error if the
	// deletion failed. The operation must be atomic, offsets must either be
	// fully removed or not, it cannot partially succeed. The operation must
	// also be idempotent, it cannot return an error if some offsets did not
	// exist in the store.
	DeleteOffsets(offsets ...Offset) error
}

// NewOffsetStore constructs and return an OffsetStore value which keeps track
// of offsets in memory.
func NewOffsetStore() OffsetStore {
	return &offsetStore{
		offsets: make(map[int64]int64),
	}
}

type offsetStore struct {
	mutex   sync.Mutex
	offsets map[int64]int64
}

func (st *offsetStore) ReadOffsets() OffsetIter {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	if len(st.offsets) == 0 {
		return &emptyOffsetIter{}
	}

	it := &offsetIter{
		offsets: make([]offset, 0, len(st.offsets)),
	}

	for value, time := range st.offsets {
		it.offsets = append(it.offsets, offset{
			value: value,
			time:  time,
		})
	}

	return it
}

func (st *offsetStore) WriteOffsets(offsets ...Offset) error {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	for i := range offsets {
		off := makeOffset(offsets[i])
		st.offsets[off.value] = off.time
	}

	return nil
}

func (st *offsetStore) DeleteOffsets(offsets ...Offset) error {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	for _, off := range offsets {
		delete(st.offsets, off.Value)
	}

	return nil
}

// OffsetIter is an interface abstracting the concept of a sequence of offsets
// which can be read progressively.
//
// A typical usage of iterator types is to read offsets from an OffsetStore and
// iterate over the results with a for loop, for example:
//
//	it := store.ReadOffsets()
//
//	for it.Next() {
//		x := i.Offset()
//		...
//	}
//
//	if err := i.Err(); err != nil {
//		...
//	}
//
// Implementations of OffsetIter do not need to be safe to use concurrently from
// multiple goroutines.
type OffsetIter interface {
	// Next moves the iterator forward and returns true if one more offset is
	// available.
	Next() bool

	// Offset returns the value of the offset currently pointed by the iterator,
	// which will be the zero-value if the previous call to Next returned false.
	Offset() Offset

	// Err returns the last non-nil error seen by the iterator.
	Err() error
}

// NewOffsetIter constructs an in-memory OffsetIter from a list of offsets.
func NewOffsetIter(offsets ...Offset) OffsetIter {
	if len(offsets) == 0 {
		return &emptyOffsetIter{}
	}

	it := &offsetIter{
		offsets: make([]offset, len(offsets)),
	}

	for i, off := range offsets {
		it.offsets[i] = makeOffset(off)
	}

	return it
}

// emptyOffsetIter is an implementation of the OffsetIter interface which
// produces no offsets. Since the type is zero-bytes it also uses no memory
// and requires no heap allocation when constructing an empty offset iterator.
type emptyOffsetIter struct{}

func (*emptyOffsetIter) Next() bool {
	return false
}

func (*emptyOffsetIter) Offset() Offset {
	return Offset{}
}

func (*emptyOffsetIter) Err() error {
	return nil
}

// errorOffsetIter is an implementation of the OffsetIter interface which only
// carries an error and produces no offsets.
type errorOffsetIter struct {
	err error
}

func (it *errorOffsetIter) Next() bool {
	return false
}

func (it *errorOffsetIter) Offset() Offset {
	return Offset{}
}

func (it *errorOffsetIter) Err() error {
	return it.err
}

// offsetIter is an imlpementation of the OffsetIter interface which
// produces offsets from an in-memory inMemorys.
type offsetIter struct {
	offsets []offset
	index   int
}

func (it *offsetIter) Next() bool {
	if it.index < len(it.offsets) {
		it.index++
		return true
	}
	return false
}

func (it *offsetIter) Offset() Offset {
	if i := it.index - 1; i < len(it.offsets) {
		return it.offsets[i].toOffset()
	}
	return Offset{}
}

func (it *offsetIter) Err() error {
	return nil
}

// offset is a type having a similar purpose to the exported Offset type, but
// it carries the pair of the offset value and time as two integers instead of
// an integer and a time.Time value, which makes it a more compact in-memory
// representation. Since it contains no pointers it also means that the garbage
// collectors doesn't need to scan memory areas that contain instances of this
// type, which can be helpful when storing slices for example.
type offset struct {
	value int64
	time  int64
}

func makeOffset(off Offset) offset {
	return offset{
		value: off.Value,
		time:  makeTime(off.Time),
	}
}

func (off offset) toOffset() Offset {
	return Offset{
		Value: off.value,
		Time:  toTime(off.time),
	}
}

func makeTime(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func toTime(t int64) time.Time {
	if t == 0 {
		return time.Time{}
	}
	return time.Unix(0, t)
}

// OffsetLog is an implementation of the OffsetStore interface which uses a
// Kafka partition as backing store for the offsets.
//
// Ideally the partition belongs to a compacted topic, but this is not required
// and the log will work just fine with a regular topic (replays may take a bit
// longer if there are large numbers of offsets recorded in the log).
type OffsetLog struct {
	// Addresses of the Kafka brokers tha the log uses to lookup the leader of
	// the topic partition that it uses.
	Brokers []string

	// Name of the Kafka topic that the offset log uses to keep track of the
	// offsets.
	Topic string

	// Partition tha the offset log uses in the Kafka topic.
	Partition int

	// Size limit of the fetch requests to the offset log, default to 8 MB.
	MaxBytes int

	// Time limit applied to operations of the offset log.
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// Dialer used by the offset log to establish connections to the Kafka
	// brokers. If nil, DefaultDialer is used.
	Dialer *Dialer
}

const (
	defaultReadTimeout  = 3 * time.Second
	defaultWriteTimeout = 3 * time.Second
	defaultMaxBytes     = 8 * 1024 * 1024
)

// ReadOffsets satisfies the OffsetIter interface, it returns an offset iterator
// which produces all offsets in the partition read by the log.
func (log *OffsetLog) ReadOffsets() OffsetIter {
	c, err := log.dialLeader()
	if err != nil {
		return &errorOffsetIter{err}
	}
	defer c.Close()

	readTimeout := log.readTimeout()
	c.SetReadDeadline(time.Now().Add(readTimeout))

	first, last, err := c.ReadOffsets()
	if err != nil {
		return &errorOffsetIter{err}
	}

	cursor, err := c.Seek(first, SeekAbsolute)
	if err != nil {
		return &errorOffsetIter{err}
	}

	maxBytes := log.maxBytes()
	offsets := make(map[int64]int64)

	for cursor < last {
		c.SetReadDeadline(time.Now().Add(readTimeout))
		b := c.ReadBatch(8, maxBytes)

		for {
			m, err := b.ReadMessage()
			if err != nil {
				break
			}

			if cursor = m.Offset; cursor < last {
				var off offset

				if len(m.Key) != 0 {
					off.value = int64(binary.BigEndian.Uint64(m.Key))
				}

				if len(m.Value) != 0 {
					off.time = int64(binary.BigEndian.Uint64(m.Value))
					offsets[off.value] = off.time
				} else {
					delete(offsets, off.value)
				}
			}
		}

		if err := b.Close(); err != nil {
			return &errorOffsetIter{err}
		}

		cursor++
	}

	it := &offsetIter{
		offsets: make([]offset, 0, len(offsets)),
	}

	for value, time := range offsets {
		it.offsets = append(it.offsets, offset{
			value: value,
			time:  time,
		})
	}

	return it
}

// WriteOffsets satisfies the OffsetIter interface, it writes a set of offsets
// to the Kafka partition managed by the log.
func (log *OffsetLog) WriteOffsets(offsets ...Offset) error {
	return log.write(makeOffsetWriteMessages(offsets))
}

// DeleteOffsets satisfies the OffsetIter interface, it deletes the offsets from
// the Kafka partition managed by the log.
func (log *OffsetLog) DeleteOffsets(offsets ...Offset) error {
	return log.write(makeOffsetDeleteMessages(offsets))
}

func (log *OffsetLog) write(msgs []Message) error {
	c, err := log.dialLeader()
	if err != nil {
		return err
	}
	defer c.Close()
	c.SetWriteDeadline(time.Now().Add(log.writeTimeout()))
	_, err = c.WriteMessages(msgs...)
	return err
}

func (log *OffsetLog) dialer() *Dialer {
	if log.Dialer != nil {
		return log.Dialer
	}
	return DefaultDialer
}

func (log *OffsetLog) dialLeader() (*Conn, error) {
	if len(log.Brokers) == 0 {
		return nil, errors.New("no brokers configured on the offset log")
	}
	broker := log.Brokers[rand.Intn(len(log.Brokers))]
	return log.dialer().DialLeader(context.Background(), "tcp", broker, log.Topic, log.Partition)
}

func (log *OffsetLog) maxBytes() int {
	if log.MaxBytes > 0 {
		return log.MaxBytes
	}
	return defaultMaxBytes
}

func (log *OffsetLog) readTimeout() time.Duration {
	if log.ReadTimeout > 0 {
		return log.ReadTimeout
	}
	return defaultReadTimeout
}

func (log *OffsetLog) writeTimeout() time.Duration {
	if log.ReadTimeout > 0 {
		return log.ReadTimeout
	}
	return defaultWriteTimeout
}

func makeOffsetWriteMessages(offsets []Offset) []Message {
	msgs := make([]Message, len(offsets))
	data := make([]byte, 16*len(offsets))

	for i := range offsets {
		off := makeOffset(offsets[i])
		key := data[(i+i+0)*8 : (i+i+1)*8]
		val := data[(i+i+1)*8 : (i+i+2)*8]
		binary.BigEndian.PutUint64(key, uint64(off.value))
		binary.BigEndian.PutUint64(val, uint64(off.time))
		msgs[i].Key, msgs[i].Value = key, val
	}

	return msgs
}

func makeOffsetDeleteMessages(offsets []Offset) []Message {
	msgs := make([]Message, len(offsets))
	data := make([]byte, 8*len(offsets))

	for i, off := range offsets {
		key := data[(i+0)*8 : (i+1)*8]
		binary.BigEndian.PutUint64(key, uint64(off.Value))
		msgs[i].Key = key
	}

	return msgs
}
