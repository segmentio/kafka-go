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

type Index interface {
	Select(ctx context.Context, prefix []byte) Select

	Update(ctx context.Context) Update
}

type Select interface {
	io.Closer

	Next() bool

	Entry() (key []byte, offset int64)
}

type Update interface {
	io.Closer

	Insert(key []byte, offset int64) error

	Delete(key []byte) error

	DropKeys(prefix []byte) error

	DropOffsets(limit int64) error

	Commit() error
}

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
	index.keys.Range(prefix, func(key []byte, offset int64) bool {
		if !bytes.HasPrefix(key, prefix) {
			return false
		}
		index.offsets.Delete(offset)
		return true
	})
}

func (index *index) dropOffsets(_ []byte, limit int64) {
	index.offsets.Range(0, func(offset int64, key []byte) bool {
		if offset >= limit {
			return false
		}
		index.keys.Delete(key)
		return true
	})
}

func (index *index) Select(ctx context.Context, prefix []byte) Select {
	slct := &indexSelect{
		entries: make([]indexEntry, 0, 32),
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
