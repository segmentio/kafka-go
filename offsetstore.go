package kafka

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

// Offset represents the position of a message in a partition, and carries extra
// metadata used to coordinate the scheduling of those messages in a consumer.
type Offset struct {
	// Absolute value of the offset, which represents the position of a message
	// in a partition.
	Value int64

	// Number of attempts that have been made to process the message at this
	// offset.
	Attempt int

	// Size of the message that the offset was associated with.
	//
	// Having the size is important in order to optimize fetches of message
	// batches when rescheduling offsets.
	Size int

	// Timestamp associated with the offset, which represents the time at which
	// the message is supposed to be rescheduled.
	Time time.Time
}

// Equal checks if two offsets are equal.
func (off Offset) Equal(other Offset) bool {
	return off.Value == other.Value &&
		off.Attempt == other.Attempt &&
		off.Size == other.Size &&
		off.Time.Equal(other.Time)
}

// String satisfies the fmt.Stringer interface.
func (off Offset) String() string {
	return fmt.Sprintf("{ offset:%d, attempt:%d, size:%d, time:%s }",
		off.Value,
		off.Attempt,
		off.Size,
		off.Time.Format(time.RFC3339Nano))
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
		offsets: make(map[offsetKey]offsetValue),
	}
}

type offsetStore struct {
	mutex   sync.Mutex
	offsets map[offsetKey]offsetValue
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

	for key, value := range st.offsets {
		it.offsets = append(it.offsets, makeOffsetFromKeyAndValue(key, value))
	}

	return it
}

func (st *offsetStore) WriteOffsets(offsets ...Offset) error {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	for _, off := range offsets {
		key, value := makeOffset(off).toKeyAndValue()
		st.offsets[key] = value
	}

	return nil
}

func (st *offsetStore) DeleteOffsets(offsets ...Offset) error {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	for _, off := range offsets {
		delete(st.offsets, makeOffset(off).toKey())
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
//	for it.Next() {
//		x := it.Offset()
//		...
//	}
//	if err := it.Err(); err != nil {
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
// it carries the values as fixed size integers instead of more user-friendly
// types like time.Time, which makes it a more compact in-memory representation.
// Since it contains no pointers it also means that the garbage collectors
// doesn't need to scan memory areas that contain instances of this type, which
// can be helpful when storing slices for example.
type offset struct {
	value   int64
	attempt int64
	size    int64
	time    int64
}

func makeOffset(off Offset) offset {
	return offset{
		value:   off.Value,
		attempt: int64(off.Attempt),
		size:    int64(off.Size),
		time:    makeTime(off.Time),
	}
}

func makeOffsetFromKeyAndValue(key offsetKey, value offsetValue) offset {
	return offset{
		value:   int64(key),
		attempt: value.attempt,
		size:    value.size,
		time:    value.time,
	}
}

func (off offset) toOffset() Offset {
	return Offset{
		Value:   off.value,
		Size:    int(off.size),
		Attempt: int(off.attempt),
		Time:    toTime(off.time),
	}
}

func (off offset) toKey() offsetKey {
	return offsetKey(off.value)
}

func (off offset) toValue() offsetValue {
	return offsetValue{
		attempt: off.attempt,
		size:    off.size,
		time:    off.time,
	}
}

func (off offset) toKeyAndValue() (offsetKey, offsetValue) {
	return off.toKey(), off.toValue()
}

type offsetKey int64

func (k *offsetKey) readFrom(b []byte) {
	*k = offsetKey(binary.BigEndian.Uint64(b))
}

func (k offsetKey) writeTo(b []byte) {
	binary.BigEndian.PutUint64(b, uint64(k))
}

type offsetValue struct {
	attempt int64
	size    int64
	time    int64
}

func (v *offsetValue) readFrom(b []byte) {
	_ = b[23] // to help with bound check elimination
	*v = offsetValue{
		attempt: int64(binary.BigEndian.Uint64(b[:8])),
		size:    int64(binary.BigEndian.Uint64(b[8:16])),
		time:    int64(binary.BigEndian.Uint64(b[16:24])),
	}
}

func (v offsetValue) writeTo(b []byte) {
	_ = b[23] // to help with bound check elimination
	binary.BigEndian.PutUint64(b[:8], uint64(v.attempt))
	binary.BigEndian.PutUint64(b[8:16], uint64(v.size))
	binary.BigEndian.PutUint64(b[16:24], uint64(v.time))
}

const (
	sizeOfOffsetKey   = 8
	sizeOfOffsetValue = 24
	sizeOfOffset      = sizeOfOffsetKey + sizeOfOffsetValue
)

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
