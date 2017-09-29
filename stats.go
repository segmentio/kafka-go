package kafka

import (
	"sync/atomic"
	"time"
	"unsafe"
)

// DurationStats is a data structure that carries a summary of observed duration
// values. The average, minimum, maximum, sum, and count are reported.
type DurationStats struct {
	Avg time.Duration `metric:"avg" type:"gauge"`
	Min time.Duration `metric:"min" type:"gauge"`
	Max time.Duration `metric:"max" type:"gauge"`
}

type durationStats struct {
	min   minimum
	max   maximum
	sum   counter
	count counter
}

func makeDurationStats() durationStats {
	return durationStats{
		min: -1,
		max: -1,
	}
}

func (d *durationStats) observe(v time.Duration) {
	d.min.observe(int64(v))
	d.max.observe(int64(v))
	d.sum.observe(int64(v))
	d.count.observe(1)
}

func (d *durationStats) snapshot() DurationStats {
	min := d.min.snapshot()
	max := d.max.snapshot()
	sum := d.sum.snapshot()
	count := d.count.snapshot()
	return DurationStats{
		Avg: time.Duration(float64(sum) / float64(count)),
		Min: time.Duration(min),
		Max: time.Duration(max),
	}
}

// counter is an atomic incrementing counter which gets reset on snapshot.
type counter int64

func (c *counter) ptr() *int64 {
	return (*int64)(unsafe.Pointer(c))
}

func (c *counter) observe(v int64) {
	atomic.AddInt64(c.ptr(), v)
}

func (c *counter) snapshot() int64 {
	p := c.ptr()
	v := atomic.LoadInt64(p)
	atomic.AddInt64(p, -v)
	return v
}

// gauge is an atomic integer that may be set to any arbitrary value, the value
// does not change after a snapshot.
type gauge int64

func (g *gauge) ptr() *int64 {
	return (*int64)(unsafe.Pointer(g))
}

func (g *gauge) observe(v int64) {
	atomic.StoreInt64(g.ptr(), v)
}

func (g *gauge) snapshot() int64 {
	return atomic.LoadInt64(g.ptr())
}

// minimum is an atomic integral type that keeps track of the minimum of all
// values that it observed between snapshots.
type minimum int64

func (m *minimum) ptr() *int64 {
	return (*int64)(unsafe.Pointer(m))
}

func (m *minimum) observe(v int64) {
	for {
		ptr := m.ptr()
		min := atomic.LoadInt64(ptr)

		if min >= 0 && min <= v {
			break
		}

		if atomic.CompareAndSwapInt64(ptr, min, v) {
			break
		}
	}
}

func (m *minimum) snapshot() int64 {
	p := m.ptr()
	v := atomic.LoadInt64(p)
	atomic.CompareAndSwapInt64(p, v, -1)
	if v < 0 {
		v = 0
	}
	return v
}

// maximum is an atomic integral type that keeps track of the maximum of all
// values that it observed between snapshots.
type maximum int64

func (m *maximum) ptr() *int64 {
	return (*int64)(unsafe.Pointer(m))
}

func (m *maximum) observe(v int64) {
	for {
		ptr := m.ptr()
		max := atomic.LoadInt64(ptr)

		if max >= 0 && max >= v {
			break
		}

		if atomic.CompareAndSwapInt64(ptr, max, v) {
			break
		}
	}
}

func (m *maximum) snapshot() int64 {
	p := m.ptr()
	v := atomic.LoadInt64(p)
	atomic.CompareAndSwapInt64(p, v, -1)
	if v < 0 {
		v = 0
	}
	return v
}
