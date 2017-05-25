package kafka

import (
	"io"
	"sync"
)

// A BatchReader represents a batch of messages reader from a kafka connection.
type BatchReader struct {
	rlock  sync.Mutex
	mutex  sync.Mutex
	conn   *Conn
	op     *connOp
	id     int32
	offset int64
	err    error
	update bool
}

func (batch *BatchReader) Close() error {
	batch.mutex.Lock()
	err := batch.err
	if batch.conn != nil {
		batch.op.cancel(err)
		batch.conn.unsetConnReadDeadline()
		batch.conn.removeOperation(batch.id)
		batch.conn = nil
	}
	batch.mutex.Unlock()
	if err == io.EOF {
		err = nil
	}
	return err
}

func (batch *BatchReader) Read(b []byte) (int, error) {
	batch.mutex.Lock()
	err := batch.err
	batch.mutex.Unlock()
	if err != nil {
		return 0, err
	}

	batch.rlock.Lock()
	// TODO
	batch.rlock.Unlock()

	return 0, io.EOF
}

func (batch *BatchReader) Offset() int64 {
	batch.mutex.Lock()
	offset := batch.offset
	batch.mutex.Unlock()
	return offset
}

// A BatchWriter exposes an API for writing batches of messages to a kafka
// connection.
type BatchWriter struct {
}

func (batch *BatchWriter) Close() error {
	return nil
}

func (batch *BatchWriter) Flush() error {
	return nil
}

func (batch *BatchWriter) Write(b []byte) (int, error) {
	return 0, nil
}
