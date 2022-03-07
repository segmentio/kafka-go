package records

import (
	"context"
	"sync"
	"sync/atomic"
)

// once is similar to sync.Once but the state will be left unchanged if the
// initialization callback errors or panics.
type once struct {
	done  int32
	mutex sync.Mutex
}

func (once *once) do(f func() error) error {
	if atomic.LoadInt32(&once.done) == 0 {
		return once.syncAndDo(f)
	}
	return nil
}

//go:noinline
func (once *once) syncAndDo(f func() error) error {
	once.mutex.Lock()
	defer once.mutex.Unlock()

	if atomic.LoadInt32(&once.done) == 0 {
		if err := f(); err != nil {
			return err
		}
		atomic.StoreInt32(&once.done, 1)
	}

	return nil
}

// barrier is a synchronization primitive intended to help synchronizing views
// of reads and writes on a kafka partition. The barriers track two offsets,
// the last offset known to the application and the last stable offset that
// writes have been observed on. When syncing on the barrier, the goroutine is
// blocked until the stable offset reaches the last offset.
type barrier struct {
	mutex  sync.Mutex
	last   int64
	stable int64
	wait   chan struct{}
}

func (b *barrier) offsets() (stableOffset, lastOffset int64) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.stable, b.last
}

func (b *barrier) observeLastOffset(offset int64) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.last < offset {
		b.last = offset
	}
}

func (b *barrier) observeStableOffset(offset int64) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.last < offset {
		b.last = offset
	}

	if b.stable < offset {
		b.stable = offset

		if b.wait != nil {
			close(b.wait)
			b.wait = nil
		}
	}
}

func (b *barrier) sync(ctx context.Context) error {
	b.mutex.Lock()
	synced := b.stable >= b.last
	b.mutex.Unlock()

	if synced {
		return nil
	}

	for {
		b.mutex.Lock()
		synced := b.stable >= b.last
		if !synced && b.wait == nil {
			b.wait = make(chan struct{})
		}
		wait := b.wait
		b.mutex.Unlock()

		if synced {
			return nil
		}

		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
