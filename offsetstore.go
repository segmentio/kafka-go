package kafka

import (
	"encoding/binary"
	"sync"
	"time"
)

// offsetStore is an interface implemented by types that support storing and
// retrieving sequences of offsets.
//
// Implementations of the offsetStore interface must be safe to use concurrently
// from multiple goroutines.
type offsetStore interface {
	// Returns an iterator over the sequence of offsets in the store.
	readOffsets() ([]offset, error)

	// Write a sequence of offsets to the store, or return an error if storing
	// the offsets failed. The operation must be atomic, offsets must either be
	// fully written or not, it cannot partially succeed. The operation must
	// also be idempotent, it cannot return an error if some offsets already
	// existed in the store.
	writeOffsets(offsets ...offset) error

	// Delete a sequence of offsets from the store, or return an error if the
	// deletion failed. The operation must be atomic, offsets must either be
	// fully removed or not, it cannot partially succeed. The operation must
	// also be idempotent, it cannot return an error if some offsets did not
	// exist in the store.
	deleteOffsets(offsets ...offset) error
}

// newOffsetStore constructs and return an OffsetStore value which keeps track
// of offsets in memory.
func newOffsetStore() offsetStore {
	return &defaultOffsetStore{
		offsets: make(map[offsetKey]offsetValue),
	}
}

type defaultOffsetStore struct {
	mutex   sync.Mutex
	offsets map[offsetKey]offsetValue
}

func (st *defaultOffsetStore) readOffsets() ([]offset, error) {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	if len(st.offsets) == 0 {
		return nil, nil
	}

	offsets := make([]offset, 0, len(st.offsets))

	for key, value := range st.offsets {
		offsets = append(offsets, makeOffsetFromKeyAndValue(key, value))
	}

	return offsets, nil
}

func (st *defaultOffsetStore) writeOffsets(offsets ...offset) error {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	for _, off := range offsets {
		key, value := off.toKeyAndValue()
		st.offsets[key] = value
	}

	return nil
}

func (st *defaultOffsetStore) deleteOffsets(offsets ...offset) error {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	for _, off := range offsets {
		delete(st.offsets, off.toKey())
	}

	return nil
}

// offset is a type carrying a message offset, attempt number, size, and time at
// which the message must be scheduled. It carries the values as fixed size
// integers instead of more user-friendly types like time.Time, which makes it
// a more compact in-memory representation. Since it contains no pointers it
// also means that the garbage collectors doesn't need to scan memory areas that
// contain instances of this type, which can be helpful when storing slices for
// example.
type offset struct {
	value   int64
	attempt int64
	size    int64
	time    int64
}

func makeOffsetFromKeyAndValue(key offsetKey, value offsetValue) offset {
	return offset{
		value:   int64(key),
		attempt: value.attempt,
		size:    value.size,
		time:    value.time,
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
