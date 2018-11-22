package kafka

import (
	"context"
	"io"
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
	offch    <-chan Offset
	errch    <-chan error
	synch    <-chan struct{}
	cancel   context.CancelFunc
	period   time.Duration
	store    OffsetStore
	timeline OffsetTimeline
}

// NewOffsetScheduler constructs an offset scheduler which schedules offsets
// on the given period and using the given offset store.
//
// The period may be zero or a negative value to indicate that the scheduler
// will use a default scheduling period.
//
// The store may be nil to indicate that the scheduler does not need to persist
// the scheduled offsets.
func NewOffsetScheduler(period time.Duration, store OffsetStore) *OffsetScheduler {
	offch := make(chan Offset)
	errch := make(chan error)
	synch := make(chan struct{})

	if period <= 0 {
		period = 100 * time.Millisecond
	}

	ctx, cancel := context.WithCancel(context.Background())

	sched := &OffsetScheduler{
		offch:  offch,
		errch:  errch,
		synch:  synch,
		cancel: cancel,
		period: period,
		store:  store,
	}

	go sched.run(ctx, offch, errch, synch)
	return sched
}

// Stop must be called when the scheduler is not needed anymore to release its
// internal resources.
//
// Calling Stop will unblock all goroutines currently waiting in a call to Next
// or Sync.
func (sched *OffsetScheduler) Stop() {
	sched.cancel()
}

// Sync blocks until the scheduler has fully synced with its offset store, or an
// error occured.
//
// Calling this method is optional, the program can call Next only which will
// already wait for the sync to complete before returning offsets.
func (sched *OffsetScheduler) Sync() error {
	select {
	case <-sched.synch:
		return nil
	case err := <-sched.errch:
		return err
	}
}

// Next returns the next offset that the scheduler picked.
//
// The method will block until the next offset becomes available. It returns a
// non-nil error if the scheduler's internal process has encountered an issue,
// or if the scheduler has been closed (in which case the error is io.EOF).
func (sched *OffsetScheduler) Next() (Offset, error) {
	var off Offset
	var err error
	var ok1 bool
	var ok2 bool

	select {
	case off, ok1 = <-sched.offch:
	case err, ok2 = <-sched.errch:
	}

	if !(ok1 || ok2) {
		err = io.EOF
	}

	return off, err
}

// Schedule adds the given offsets to the scheduler, they will be later returned
// by a call to Next when their scheduling time has been reached.
func (sched *OffsetScheduler) Schedule(offsets ...Offset) error {
	if sched.store != nil {
		if err := sched.store.WriteOffsets(offsets...); err != nil {
			return err
		}
	}
	sched.timeline.Push(offsets...)
	return nil
}

func (sched *OffsetScheduler) run(ctx context.Context, offch chan<- Offset, errch chan<- error, synch chan<- struct{}) {
	defer close(errch)
	defer close(offch)

	ticker := time.NewTicker(sched.period)
	defer ticker.Stop()

	done := ctx.Done()
	sync := false

	for {
		var now time.Time
		var err error

		if !sync {
			sync, err = sched.sync()

			if sync {
				close(synch)
			}
		}

		if err != nil {
			select {
			case errch <- err:
			case <-done:
				return
			}
		}

		select {
		case now = <-ticker.C:
		case <-done:
			return
		}

		if !sched.schedule(now, offch, done) {
			return
		}
	}
}

func (sched *OffsetScheduler) sync() (bool, error) {
	if sched.store == nil {
		return true, nil
	}

	it := sched.store.ReadOffsets()
	offsets := make([]offset, 0, 1000)

	for it.Next() {
		offsets = append(offsets, makeOffset(it.Offset()))
	}

	if err := it.Err(); err != nil {
		return false, err
	}

	for _, off := range offsets {
		sched.timeline.Push(off.toOffset())
	}

	return true, nil
}

func (sched *OffsetScheduler) schedule(now time.Time, offch chan<- Offset, done <-chan struct{}) bool {
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
		case offch <- offset:
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
