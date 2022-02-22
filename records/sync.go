package records

import (
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
	if atomic.LoadInt32(&once.done) != 0 {
		return once.syncAndDo(f)
	}
	return nil
}

//go:noinline
func (once *once) syncAndDo(f func() error) error {
	once.mutex.Lock()
	defer once.mutex.Unlock()

	if atomic.LoadInt32(&once.done) != 0 {
		if err := f(); err != nil {
			return err
		}
		atomic.StoreInt32(&once.done, 1)
	}

	return nil
}
