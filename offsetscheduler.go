package kafka

import (
	"context"
	"time"
)

// offsetScheduler is a type which implements offset time management.
//
// offsetScheduler values are safe to use concurrently from multiple goroutines.
type offsetScheduler struct {
	offch    chan Offset
	cancel   context.CancelFunc
	period   time.Duration
	timeline offsetTimeline
}

// newOffsetScheduler constructs an offset scheduler which schedules offsets
// on the given period.
//
// The period must be greater than zero or the function will panic.
func newOffsetScheduler(period time.Duration) *offsetScheduler {
	offch := make(chan Offset)

	if period <= 0 {
		panic("invalid offset scheduler period")
	}

	ctx, cancel := context.WithCancel(context.Background())

	sched := &offsetScheduler{
		offch:  offch,
		cancel: cancel,
		period: period,
	}

	go sched.run(ctx)
	return sched
}

// Len returns the number of offsets managed by the scheduler.
func (sched *offsetScheduler) Len() int { return sched.timeline.Len() }

// Stop must be called when the scheduler is not needed anymore to release its
// internal resources.
//
// Calling Stop will unblock all goroutines currently waiting on offsets to be
// scheduled.
func (sched *offsetScheduler) Stop() { sched.cancel() }

// Next exposes a read-only channel that the scheduler produces offsets to be
// scheduled on.
//
// The method will block until the next offset becomes available.
func (sched *offsetScheduler) Offsets() <-chan Offset { return sched.offch }

// Schedule adds the given offsets to the scheduler, they will be later returned
// by a call to Next when their scheduling time has been reached.
func (sched *offsetScheduler) Schedule(offsets ...Offset) { sched.timeline.Push(offsets...) }

func (sched *offsetScheduler) run(ctx context.Context) {
	defer close(sched.offch)

	ticker := time.NewTicker(sched.period)
	defer ticker.Stop()

	now := time.Now()
	done := ctx.Done()

	for sched.schedule(now, done) {
		select {
		case now = <-ticker.C:
		case <-done:
			return
		}
	}
}

func (sched *offsetScheduler) schedule(now time.Time, done <-chan struct{}) bool {
	var offsets = sched.timeline.Pop(now)

	if len(offsets) == 0 {
		return true
	}

	for _, offset := range offsets {
		select {
		case sched.offch <- offset:
		case <-done:
			return false
		}
	}

	return true
}
