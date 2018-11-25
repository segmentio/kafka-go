package kafka

import (
	"context"
	"sync"
	"time"
)

// offsetScheduler is a type which implements offset time management.
//
// offsetScheduler values are safe to use concurrently from multiple goroutines.
type offsetScheduler struct {
	// immutable configuration of the offset scheduler.
	period time.Duration
	// mutable statet of the offset scheduler, lazily initialized on first use,
	// synchronized on the sync.Once value.
	once   sync.Once
	offch  chan []offset
	cancel context.CancelFunc
	// offsetTimeline is safe to use concurrently from multiple goroutines.
	timeline offsetTimeline
}

// len returns the number of offsets managed by the scheduler.
func (sched *offsetScheduler) len() int { return sched.timeline.len() }

// stop must be called when the scheduler is not needed anymore to release its
// internal resources.
//
// Calling stop will unblock all goroutines currently waiting on offsets to be
// scheduled.
func (sched *offsetScheduler) stop() {
	sched.once.Do(func() {
		sched.offch = make(chan []offset)
		close(sched.offch)
	})

	if sched.cancel != nil {
		sched.cancel()
	}
}

// offsets exposes a read-only channel that the scheduler produces offsets to be
// scheduled on.
//
// The method will block until the next batch of offsets becomes available.
func (sched *offsetScheduler) offsets() <-chan []offset {
	sched.once.Do(sched.start)
	return sched.offch
}

// schedule adds the given offsets to the scheduler, they will be later returned
// by a call to Next when their scheduling time has been reached.
func (sched *offsetScheduler) schedule(offsets ...offset) {
	sched.once.Do(sched.start)
	sched.timeline.push(offsets...)
}

func (sched *offsetScheduler) start() {
	period := sched.period
	if period <= 0 {
		period = time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	sched.offch = make(chan []offset)
	sched.cancel = cancel

	go sched.run(ctx)
}

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
