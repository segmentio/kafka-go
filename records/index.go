package records

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/segmentio/datastructures/v2/compare"
	"github.com/segmentio/datastructures/v2/container/tree"
)

var (
	errCommitted = errors.New("records index update was already committed")
)

// The Index interface represents an index (and reverse index) of Kafka message
// keys to offsets.
//
// Index instances must be safe to use concurrently from multiple goroutines.
type Index interface {
	// Returns a selection of keys that match the given prefix.
	//
	// The returned Select object must be closed by the program when it is not
	// needed anymore in order to release system resources allocated to hold
	// the query results.
	Select(ctx context.Context, prefix []byte) Select

	// Performs an update on the index, returning an Update object which can be
	// used apply mutations.
	//
	// The context passed to the method is captured by the returned Update and
	// will be used to abort the transaction if it is canceled before applying
	// the update.
	//
	// Updates are performed atomically on the index when committed.
	//
	// The returned Update object must be closed when the program does not need
	// it anymore in order to release system resources allocated for the
	// transaction.
	Update(ctx context.Context) Update
}

// The Select interface is an iterator exposing the results of selecting a set
// of keys by prefix on an index.
//
// Selections must be closed when the program has finished using the values.
// If an error occured while iterating over the query results, it will be
// returned by the call to Close.
type Select interface {
	io.Closer

	// Next is called to advance the selection to the next position.
	//
	// Newly created selections are positioned before the first result, the Next
	// method must then be called before the first call to Entry.
	Next() bool

	// Entry is called to return the pair of key and offset that the selection
	// is currently positioned on. The returned key must be treated as an
	// immutable value by the application, and remains valid until the next call
	// to Next or Close.
	Entry() (key []byte, offset int64)
}

// The Update interface represents a transaction that updates an index.
type Update interface {
	io.Closer

	// Inserts a new entry into the index.
	Insert(key []byte, offset int64) error

	// Deletes a key from the index.
	Delete(key []byte) error

	// Drops a key prefix from the index.
	DropKeys(prefix []byte) error

	// Drops offsets up to the given limit from the index.
	DropOffsets(limit int64) error

	// Commits the update to the index.
	Commit() error
}

// NewIndex constructs a new in-memory index.
func NewIndex() Index { return new(index) }

type index struct {
	mutex   sync.RWMutex
	keys    tree.Map[[]byte, int64]
	offsets tree.Map[int64, []byte]
}

func (index *index) commit(operations []indexOperation) {
	index.mutex.Lock()
	defer index.mutex.Unlock()

	for _, op := range operations {
		op.function(index, op.key, op.offset)
	}
}

func (index *index) insert(key []byte, offset int64) {
	if index.keys.Len() == 0 {
		index.keys.Init(bytes.Compare)
	}
	if index.offsets.Len() == 0 {
		index.offsets.Init(compare.Function[int64])
	}
	index.keys.Insert(key, offset)
	index.offsets.Insert(offset, key)
}

func (index *index) delete(key []byte, _ int64) {
	if offset, ok := index.keys.Delete(key); ok {
		index.offsets.Delete(offset)
	}
}

func (index *index) dropKeys(prefix []byte, _ int64) {
	keys := make([][]byte, 0, 42)
	index.keys.Range(prefix, func(key []byte, offset int64) bool {
		if !bytes.HasPrefix(key, prefix) {
			return false
		}
		keys = append(keys, key)
		index.offsets.Delete(offset)
		return true
	})
	for _, key := range keys {
		index.keys.Delete(key)
	}
}

func (index *index) dropOffsets(_ []byte, limit int64) {
	offsets := make([]int64, 0, 128)
	index.offsets.Range(0, func(offset int64, key []byte) bool {
		if offset >= limit {
			return false
		}
		offsets = append(offsets, offset)
		index.keys.Delete(key)
		return true
	})
	for _, offset := range offsets {
		index.offsets.Delete(offset)
	}
}

func (index *index) Select(ctx context.Context, prefix []byte) Select {
	slct := &indexSelect{
		entries: make([]indexEntry, 0, 32),
		index:   -1,
	}

	index.mutex.RLock()
	defer index.mutex.RUnlock()

	index.keys.Range(prefix, func(key []byte, offset int64) bool {
		if !bytes.HasPrefix(key, prefix) {
			return false
		}
		slct.entries = append(slct.entries, indexEntry{
			key:    key,
			offset: offset,
		})
		return true
	})

	return slct
}

func (index *index) Update(ctx context.Context) Update {
	return &indexUpdate{
		ctx:   ctx,
		index: index,
	}
}

type indexSelect struct {
	entries []indexEntry
	index   int
}

type indexEntry struct {
	key    []byte
	offset int64
}

func (slct *indexSelect) Close() error {
	slct.entries, slct.index = nil, 0
	return nil
}

func (slct *indexSelect) Next() bool {
	slct.index++
	return slct.index < len(slct.entries)
}

func (slct *indexSelect) Entry() (key []byte, offset int64) {
	if i := slct.index; i >= 0 && i < len(slct.entries) {
		return slct.entries[i].key, slct.entries[i].offset
	}
	return nil, 0
}

type indexUpdate struct {
	ctx        context.Context
	index      *index
	operations []indexOperation
}

type indexOperation struct {
	function func(*index, []byte, int64)
	key      []byte
	offset   int64
}

func (update *indexUpdate) Close() error {
	update.index = nil
	return nil
}

func (update *indexUpdate) Commit() error {
	if err := update.check(); err != nil {
		return err
	}
	defer func() { update.index = nil }()
	update.index.commit(update.operations)
	return nil
}

func (update *indexUpdate) Insert(key []byte, offset int64) error {
	return update.push((*index).insert, copyBytes(key), offset)
}

func (update *indexUpdate) Delete(key []byte) error {
	return update.push((*index).delete, copyBytes(key), -1)
}

func (update *indexUpdate) DropKeys(prefix []byte) error {
	return update.push((*index).dropKeys, copyBytes(prefix), -1)
}

func (update *indexUpdate) DropOffsets(limit int64) error {
	return update.push((*index).dropOffsets, nil, limit)
}

func (update *indexUpdate) check() error {
	if update.index == nil {
		return errCommitted
	}
	return update.ctx.Err()
}

func (update *indexUpdate) push(function func(*index, []byte, int64), key []byte, offset int64) error {
	if err := update.check(); err != nil {
		return err
	}
	update.operations = append(update.operations, indexOperation{
		function: function,
		key:      key,
		offset:   offset,
	})
	return nil
}

func copyBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
