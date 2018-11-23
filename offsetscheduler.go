package kafka

import (
	"context"
	"time"
)

// offsetScheduler is a type which implements offset time management.
//
// offsetScheduler values are safe to use concurrently from multiple goroutines.
type offsetScheduler struct {
	offch    chan []offset
	cancel   context.CancelFunc
	period   time.Duration
	timeline offsetTimeline
}

// newOffsetScheduler constructs an offset scheduler which schedules offsets
// on the given period.
//
// The period must be greater than zero or the function will panic.
func newOffsetScheduler(period time.Duration) *offsetScheduler {
	offch := make(chan []offset)

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

// len returns the number of offsets managed by the scheduler.
func (sched *offsetScheduler) len() int { return sched.timeline.len() }

// stop must be called when the scheduler is not needed anymore to release its
// internal resources.
//
// Calling stop will unblock all goroutines currently waiting on offsets to be
// scheduled.
func (sched *offsetScheduler) stop() { sched.cancel() }

// offsets exposes a read-only channel that the scheduler produces offsets to be
// scheduled on.
//
// The method will block until the next batch of offsets becomes available.
func (sched *offsetScheduler) offsets() <-chan []offset { return sched.offch }

// schedule adds the given offsets to the scheduler, they will be later returned
// by a call to Next when their scheduling time has been reached.
func (sched *offsetScheduler) schedule(offsets ...offset) { sched.timeline.push(offsets...) }

func (sched *offsetScheduler) run(ctx context.Context) {
	defer close(sched.offch)

	ticker := time.NewTicker(sched.period)
	defer ticker.Stop()

	now := time.Now()
	done := ctx.Done()

	for {
		if offsets := sched.timeline.pop(now); len(offsets) != 0 {
			select {
			case sched.offch <- offsets:
			case <-done:
				return
			}
		}

		select {
		case now = <-ticker.C:
		case <-done:
			return
		}
	}
}
