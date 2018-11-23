package kafka

import (
	"container/heap"
	"sync"
	"time"
)

// offsetTimeline is a data structure which keeps track of offsets ordered by
// time. It is intended to act as a priority queue to schedule times at which
// offsets are expected to be retried.
//
// No deduplication of values is done by this data structure, it only ensures
// the ordering of the elements that are inserted.
//
// offsetTimeline values are safe to use concurrently from multiple goroutines.
type offsetTimeline struct {
	mutex  sync.Mutex
	queue  offsetTimelineQueue
	offset offset
}

// Len returns the number of offsets stored in the timeline.
func (t *offsetTimeline) len() int {
	t.mutex.Lock()
	n := t.queue.Len()
	t.mutex.Unlock()
	return n
}

// push adds one or more offsets to the timeline.
func (t *offsetTimeline) push(offsets ...offset) {
	t.mutex.Lock()

	for _, off := range offsets {
		// Use a temporary variable allocated in the offsetTimeline struct to
		// avoid the dynamic memory allocation that would occur otherwise to
		// convert the offset to an empty interface.
		t.offset = off
		heap.Push(&t.queue, &t.offset)
	}

	t.mutex.Unlock()
}

// pop removes all offsets with a time less or equal to `now` from the timeline.
//
// The removed offsets are returned in a slice sorted by order of priority.
func (t *offsetTimeline) pop(now time.Time) []offset {
	var offsets []offset
	var unixNow = now.UnixNano()
	t.mutex.Lock()

	for t.queue.Len() > 0 {
		// Pop returns the offset as a pointer into the internal offset slice
		// of the queue to avoid the dynamoc memory allocation that would occur
		// during the conversion to an empty interface if the offset was passed
		// by value.
		off := *(heap.Pop(&t.queue).(*offset))

		if off.time <= unixNow {
			offsets = append(offsets, off)
		} else {
			t.offset = off
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
