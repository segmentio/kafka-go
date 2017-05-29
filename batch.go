package kafka

import "sync"

type Batch struct {
	conn *Conn
	lock *sync.Mutex
}

func (batch *Batch) Read(b []byte) (int, error) {
	return 0, nil
}
