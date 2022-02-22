//go:generate gotemplate "github.com/ncw/gotemplate/treemap" "index(*entry, struct{})"

// Package cache contains data structures used in the impementation of the
// records cache.
package cache

import "container/list"

// Key represents the keys by which records are indexed in the cache.
type Key struct {
	Addr       string
	Topic      string
	Partition  int
	BaseOffset int64
}

// Less compares two keys and returns true if k1 < k2.
func (k1 *Key) Less(k2 *Key) bool {
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

// LRU is the implementation of an index associating keys to the size of their
// value, and allowing eviction of least recently used keys.
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
		lru.index = newIndex(func(e1, e2 *entry) bool {
			return e1.key.Less(&e2.key)
		})
	}
	e := &entry{key: key, size: size}
	lru.size += size
	lru.index.Set(e, struct{}{})
	e.elem = lru.queue.PushFront(e)
}

// Lookup looks for the given key in the index, returning the first key that was
// greater or equal.
func (lru *LRU) Lookup(key Key) (Key, bool) {
	if lru.index != nil {
		// TODO: can we prevent the entry value from escaping?
		it := lru.index.LowerBound(&entry{key: key})
		if it.Valid() {
			e := it.Key()
			lru.queue.MoveToFront(e.elem)
			return e.key, true
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
