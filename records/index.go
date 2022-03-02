package records

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/emirpasic/gods/trees/redblacktree"
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
	keys    *redblacktree.Tree
	offsets *redblacktree.Tree
}

type indexEntry struct {
	key    string
	offset int64
}

func (index *index) commit(operations []indexOperation) {
	index.mutex.Lock()
	defer index.mutex.Unlock()

	for i := range operations {
		operations[i].function(index, &operations[i].key, &operations[i].offset)
	}
}

func (index *index) insert(key *string, offset *int64) {
	if index.keys == nil {
		index.keys = redblacktree.NewWith(func(a, b interface{}) int {
			k1, k2 := a.(*string), b.(*string)
			return strings.Compare(*k1, *k2)
		})
	}
	if index.offsets == nil {
		index.offsets = redblacktree.NewWith(func(a, b interface{}) int {
			k1, k2 := a.(*int64), b.(*int64)
			return int(*k1 - *k2)
		})
	}
	entry := &indexEntry{
		key:    *key,
		offset: *offset,
	}
	index.keys.Put(&entry.key, entry)
	index.offsets.Put(&entry.offset, entry)
}

func (index *index) delete(key *string, _ *int64) {
	if index.keys != nil {
		if value, ok := index.keys.Get(key); ok {
			entry := value.(*indexEntry)
			index.keys.Remove(&entry.key)
			index.offsets.Remove(&entry.offset)
		}
	}
}

func (index *index) dropKeys(prefix *string, _ *int64) {
	if index.keys != nil {
		entries := make([]*indexEntry, 0, index.keys.Size())

		it := index.keys.Iterator()
		it.Begin()

		for it.Next() {
			if key := *(it.Key().(*string)); strings.HasPrefix(key, *prefix) {
				entries = append(entries, it.Value().(*indexEntry))
			}
		}

		index.drop(entries)
	}
}

func (index *index) dropOffsets(_ *string, limit *int64) {
	if index.offsets != nil {
		entries := make([]*indexEntry, 0, index.offsets.Size())

		it := index.offsets.Iterator()
		it.Begin()

		for it.Next() {
			if offset := *(it.Key().(*int64)); offset >= *limit {
				break
			}
			entries = append(entries, it.Value().(*indexEntry))
		}

		index.drop(entries)
	}
}

func (index *index) drop(entries []*indexEntry) {
	for _, entry := range entries {
		index.keys.Remove(&entry.key)
		index.offsets.Remove(&entry.offset)
	}
}

func (index *index) Select(ctx context.Context, prefix []byte) Select {
	index.mutex.RLock()
	defer index.mutex.RUnlock()
	return &indexSelect{}
}

func (index *index) Update(ctx context.Context) Update {
	return &indexUpdate{
		ctx:   ctx,
		index: index,
	}
}

type indexSelect struct {
	entries []*indexEntry
	index   int
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
		return []byte(slct.entries[i].key), slct.entries[i].offset
	}
	return nil, 0
}

type indexUpdate struct {
	ctx        context.Context
	index      *index
	operations []indexOperation
}

type indexOperation struct {
	function func(*index, *string, *int64)
	key      string
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
	return update.push((*index).insert, key, offset)
}

func (update *indexUpdate) Delete(key []byte) error {
	return update.push((*index).delete, key, -1)
}

func (update *indexUpdate) DropKeys(prefix []byte) error {
	return update.push((*index).dropKeys, prefix, -1)
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

func (update *indexUpdate) push(function func(*index, *string, *int64), key []byte, offset int64) error {
	if err := update.check(); err != nil {
		return err
	}
	update.operations = append(update.operations, indexOperation{
		function: function,
		key:      string(key),
		offset:   offset,
	})
	return nil
}
