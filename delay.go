package kafka

import (
	"context"
	"sync"
)

// Delay is a synchronization primitive that blocks readers until a value
// is delivered or the delay is cancelled. A Delay is one-shot - create a
// new Delay instead of resetting.
//
// Usage:
//   - Call Deliver(v) to set the value and unblock all waiters
//   - Call Cancel() to unblock waiters without a value (returns false from Get)
//   - Call Get(ctx) to block until delivered or cancelled
//   - Call GetIfDelivered() for non-blocking check
//
// Deliver and Cancel race - only the first one wins.
type Delay[T any] struct {
	value     T
	resolved  chan struct{}
	once      sync.Once
	delivered bool
}

// NewDelay creates a new Delay in the undelivered state.
func NewDelay[T any]() *Delay[T] {
	return &Delay[T]{
		resolved: make(chan struct{}),
	}
}

// Deliver sets the value and unblocks all waiters.
// Only the first call to Deliver or Cancel has any effect.
func (d *Delay[T]) Deliver(v T) {
	d.once.Do(func() {
		d.value = v
		d.delivered = true
		close(d.resolved)
	})
}

// Cancel unblocks all waiters without delivering a value.
// Only the first call to Deliver or Cancel has any effect.
func (d *Delay[T]) Cancel() {
	d.once.Do(func() {
		close(d.resolved)
	})
}

// Get blocks until the value is delivered, cancelled, or ctx is cancelled.
// Returns (value, true) if delivered, (zero, false) otherwise.
func (d *Delay[T]) Get(ctx context.Context) (T, bool) {
	select {
	case <-d.resolved:
		if d.delivered {
			return d.value, true
		}
		var zero T
		return zero, false
	case <-ctx.Done():
		var zero T
		return zero, false
	}
}

// GetIfDelivered returns the value if delivered, without blocking.
// Returns (value, true) if delivered, (zero, false) if not yet delivered or cancelled.
func (d *Delay[T]) GetIfDelivered() (T, bool) {
	select {
	case <-d.resolved:
		if d.delivered {
			return d.value, true
		}
		var zero T
		return zero, false
	default:
		var zero T
		return zero, false
	}
}
