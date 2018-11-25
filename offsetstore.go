package kafka

import (
	"sync"
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
