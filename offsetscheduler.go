package kafka

import (
	"context"
	"time"
)

// OffsetScheduler is a type which implements offset time management.
//
// OffsetScheduler values are safe to use concurrently from multiple goroutines.
//
// Note: Technically we don't need this type to be part of the exported API of
// kafka-go in order to implement requeues and retries on the Reader, but it
// can be useful to have in order to build similar features in other packages.
type OffsetScheduler struct {
	offch    chan Offset
	cancel   context.CancelFunc
	period   time.Duration
	timeline OffsetTimeline
}

// NewOffsetScheduler constructs an offset scheduler which schedules offsets
// on the given period.
//
// The period may be zero or a negative value to indicate that the scheduler
// will use a default scheduling period.
func NewOffsetScheduler(period time.Duration) *OffsetScheduler {
	offch := make(chan Offset)

	if period <= 0 {
		period = 100 * time.Millisecond
	}

	ctx, cancel := context.WithCancel(context.Background())

	sched := &OffsetScheduler{
		offch:  offch,
		cancel: cancel,
		period: period,
	}

	go sched.run(ctx)
	return sched
}

// Len returns the number of offsets managed by the scheduler.
func (sched *OffsetScheduler) Len() int { return sched.timeline.Len() }

// Stop must be called when the scheduler is not needed anymore to release its
// internal resources.
//
// Calling Stop will unblock all goroutines currently waiting on offsets to be
// scheduled.
func (sched *OffsetScheduler) Stop() { sched.cancel() }

// Next exposes a read-only channel that the scheduler produces offsets to be
// scheduled on.
//
// The method will block until the next offset becomes available.
func (sched *OffsetScheduler) Offsets() <-chan Offset { return sched.offch }

// Schedule adds the given offsets to the scheduler, they will be later returned
// by a call to Next when their scheduling time has been reached.
func (sched *OffsetScheduler) Schedule(offsets ...Offset) { sched.timeline.Push(offsets...) }

func (sched *OffsetScheduler) run(ctx context.Context) {
	defer close(sched.offch)

	ticker := time.NewTicker(sched.period)
	defer ticker.Stop()

	done := ctx.Done()

	for {
		var now time.Time

		select {
		case now = <-ticker.C:
		case <-done:
			return
		}

		if !sched.schedule(now, done) {
			return
		}
	}
}

func (sched *OffsetScheduler) schedule(now time.Time, done <-chan struct{}) bool {
	var offsets = sched.timeline.Pop(now)

	if len(offsets) == 0 {
		return true
	}

	var throttlePeriod = sched.period / time.Duration(len(offsets))
	var throttleTicker *time.Ticker

	if throttlePeriod > time.Microsecond {
		throttleTicker = time.NewTicker(throttlePeriod)
		defer throttleTicker.Stop()
	}

	for _, offset := range offsets {
		select {
		case sched.offch <- offset:
		case <-done:
			return false
		}

		if throttleTicker != nil {
			select {
			case <-throttleTicker.C:
			case <-done:
				return false
			}
		}
	}

	return true
}
