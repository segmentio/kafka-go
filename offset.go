package kafka

import (
	"encoding/binary"
	"sort"
)

const (
	sizeOfOffsetKey   = 8
	sizeOfOffsetValue = 24
	sizeOfOffset      = sizeOfOffsetKey + sizeOfOffsetValue
)

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

func sortOffsets(offsets []offset) {
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i].value < offsets[j].value
	})
}

func lookupSortedOffsets(offsets []offset, off int64) (offset, bool) {
	i := searchSortedOffsets(offsets, off)
	if i < len(offsets) && offsets[i].value == off {
		return offsets[i], true
	}
	return offset{}, false
}

func sliceSortedOffsets(offsets []offset, min, max int64) []offset {
	i := searchSortedOffsets(offsets, min)
	j := searchSortedOffsets(offsets, max)
	return offsets[i:j]
}

func searchSortedOffsets(offsets []offset, off int64) int {
	return sort.Search(len(offsets), func(i int) bool {
		return offsets[i].value >= off
	})
}
