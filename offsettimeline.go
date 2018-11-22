package kafka

import (
	"container/heap"
	"sync"
	"time"
)

// OffsetTimeline is a data structure which keeps track of offsets ordered by
// time. It is intended to act as a priority queue to schedule times at which
// offsets are expected to be retried.
//
// No deduplication of values is done by this data structure, it only ensures
// the ordering of the elements that are inserted.
//
// OffsetTimeline values are safe to use concurrently from multiple goroutines.
//
// Note: Technically we don't need this type to be part of the exported API of
// kafka-go in order to implement requeues and retries on the Reader, but it
// can be useful to have in order to build similar features in other packages.
type OffsetTimeline struct {
	mutex  sync.Mutex
	queue  offsetTimelineQueue
	offset offset
}

// Len returns the number of offsets stored in the timeline.
func (t *OffsetTimeline) Len() int {
	t.mutex.Lock()
	n := t.queue.Len()
	t.mutex.Unlock()
	return n
}

// Push adds one or more offsets to the timeline.
func (t *OffsetTimeline) Push(offsets ...Offset) {
	t.mutex.Lock()

	for _, off := range offsets {
		// Use a temporary variable allocated in the OffsetTimeline struct to
		// avoid the dynamic memory allocation that would occur otherwise to
		// convert the offset to an empty interface.
		t.offset = makeOffset(off)
		heap.Push(&t.queue, &t.offset)
	}

	t.mutex.Unlock()
}

// Pop removes all offsets with a time less or equal to `now` from the timeline.
//
// The removed offsets are returned in a slice sorted by order of priority.
func (t *OffsetTimeline) Pop(now time.Time) []Offset {
	var offsets []Offset
	var unixNow = now.UnixNano()
	t.mutex.Lock()

	for t.queue.Len() > 0 {
		// Pop returns the offset as a pointer into the internal offset slice
		// of the queue to avoid the dynamoc memory allocation that would occur
		// during the conversion to an empty interface if the offset was passed
		// by value.
		off := heap.Pop(&t.queue).(*offset)

		if off.time <= unixNow {
			offsets = append(offsets, off.toOffset())
		} else {
			t.offset = *off
			heap.Push(&t.queue, &t.offset)
			break
		}
	}

	t.mutex.Unlock()
	return offsets
}

type offsetTimelineQueue struct {
	offsets []offset
}

func (q *offsetTimelineQueue) Len() int {
	return len(q.offsets)
}

func (q *offsetTimelineQueue) Less(i, j int) bool {
	return q.offsets[i].time < q.offsets[j].time
}

func (q *offsetTimelineQueue) Swap(i, j int) {
	q.offsets[i], q.offsets[j] = q.offsets[j], q.offsets[i]
}

func (q *offsetTimelineQueue) Push(x interface{}) {
	q.offsets = append(q.offsets, *x.(*offset))
}

func (q *offsetTimelineQueue) Pop() interface{} {
	i := len(q.offsets) - 1
	x := &q.offsets[i]
	q.offsets = q.offsets[:i]

	// If too much of the capacity isn't be used, reallocate the slice in a
	// smaller memory buffer.
	//
	// The 1024 limit here is kind of arbitrary, it means that we don't bother
	// resizing slices that are less than 16 KB (each element tis 16 bytes).
	if len(q.offsets) > 1024 && len(q.offsets) < (cap(q.offsets)/4) {
		offsets := q.offsets
		q.offsets = make([]offset, len(q.offsets), cap(q.offsets)/4)
		copy(q.offsets, offsets)
	}

	return x
}
