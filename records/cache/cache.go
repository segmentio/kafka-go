//go:generate gotemplate "github.com/ncw/gotemplate/treemap" "index(*entry, struct{})"

// Package cache contains data structures used in the impementation of the
// records cache.
package cache

import (
	"container/list"
	"fmt"
)

// Key represents the keys by which records are indexed in the cache.
type Key struct {
	Addr            string
	Topic           string
	Partition       int32
	LastOffsetDelta int32
	BaseOffset      int64
}

func (k Key) String() string {
	if k == (Key{}) {
		return "(none)"
	}
	return fmt.Sprintf("%s/%s.%d[%d:%d]", k.Addr, k.Topic, k.Partition, k.BaseOffset, k.endOffset())
}

// Less compares two keys and returns true if k1 < k2.
func (k1 Key) Less(k2 Key) bool {
	if k1.Addr != k2.Addr {
		return k1.Addr < k2.Addr
	}
	if k1.Topic != k2.Topic {
		return k1.Topic < k2.Topic
	}
	if k1.Partition != k2.Partition {
		return k1.Partition < k2.Partition
	}
	return k1.BaseOffset < k2.BaseOffset
}

func (k1 Key) match(k2 Key) bool {
	return k1.Addr == k2.Addr && k1.Topic == k2.Topic && k1.Partition == k2.Partition
}

func (k1 Key) contains(k2 Key) bool {
	return k1.match(k2) && k1.containsOffset(k2.BaseOffset)
}

func (k Key) containsOffset(offset int64) bool {
	return (k.BaseOffset == offset || (k.BaseOffset < offset && offset < k.endOffset()))
}

func (k Key) endOffset() int64 {
	return k.BaseOffset + int64(k.LastOffsetDelta)
}

// LRU is the implementation of an index associating keys to the size of their
// value, and allowing eviction of least recently used keys.
//
// The purpose of this data structure is not to be a general purpose LRU cache,
// the implementation makes assumptions about the use case of indexing batches
// of kafka records.
type LRU struct {
	index *index
	queue list.List
	size  int64
}

type entry struct {
	key  Key
	size int64
	elem *list.Element
}

func (e1 *entry) less(e2 *entry) bool {
	return e1.key.Less(e2.key)
}

// Reset clears the content of the data structure.
func (lru *LRU) Reset() {
	if lru.index != nil {
		lru.index.Clear()
	}
	lru.queue = list.List{}
	lru.size = 0
}

// Insert adds a key to the data structure.
func (lru *LRU) Insert(key Key, size int64) {
	if lru.index == nil {
		lru.index = newIndex()
	}
	e := &entry{key: key, size: size}
	lru.size += size
	lru.index.Set(e, struct{}{})
	e.elem = lru.queue.PushFront(e)
}

// Lookup looks for the given key in the index, returning the first key with
// offset key
func (lru *LRU) Lookup(key Key) (Key, bool) {
	if lru.index != nil {
		it := lru.index.LowerBound(&entry{key: key})
		if it.Valid() {
			e := it.Key()
			if !e.key.contains(key) {
				e = nil
				// Note: tkae some flexibility with abstraction boundaries her
				// because calling Prev on the first node panics, we have no
				// other way to determine whether it is safe to move backward.
				if prev := predecessorIndex(it.node); prev != nil {
					it.node = prev
					if e = it.Key(); !e.key.contains(key) {
						e = nil
					}
				}
			}
			if e != nil {
				lru.queue.MoveToFront(e.elem)
				return e.key, true
			}
		}
	}
	return Key{}, false
}

// Evict evicts the least recently used key, returning it along with the size
// that was associated with it.
func (lru *LRU) Evict() (Key, int64, bool) {
	if lru.queue.Len() == 0 {
		return Key{}, 0, false
	}
	e := lru.queue.Back().Value.(*entry)
	lru.index.Del(e)
	lru.queue.Remove(e.elem)
	lru.size -= e.size
	return e.key, e.size, true
}

// Len returns the number of keys currently tracked by the data structure.
func (lru *LRU) Len() int {
	return lru.queue.Len()
}

// Size returns the sum of key sizes currently tracked by the data structure.
func (lru *LRU) Size() int64 {
	return lru.size
}
