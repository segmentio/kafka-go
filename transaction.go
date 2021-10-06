package kafka

import (
	"context"
	"sync"
)

type Transaction struct {
	w               *Writer
	producerEpoch   int
	producerID      int
	transactionalID string
	doneOnce        sync.Once
	done            chan struct{}
}

func (t *Transaction) WriteMessages(ctx context.Context, msgs ...Message) error {
	// Two steps here
	// 1. AddPartitionToTxn
	// 2. Produce the messages
	// Hopefully the Writers WriteMessages and this one can share most of their code.
	return nil
}

func (t *Transaction) Rollback(ctx context.Context) error {
	defer t.doneOnce.Do(func() {
		close(t.done)
	})
	resp, err := t.w.client(t.w.writeTimeout()).EndTxn(ctx, &EndTxnRequest{
		TransactionalID: t.transactionalID,
		ProducerID:      t.producerID,
		ProducerEpoch:   t.producerEpoch,
		Committed:       false,
	})
	if err != nil {
		return err
	}
	return resp.Error
}

func (t *Transaction) Commit(ctx context.Context) error {
	defer t.doneOnce.Do(func() {
		close(t.done)
	})
	resp, err := t.w.client(t.w.writeTimeout()).EndTxn(ctx, &EndTxnRequest{
		TransactionalID: t.transactionalID,
		ProducerID:      t.producerID,
		ProducerEpoch:   t.producerEpoch,
		Committed:       true,
	})
	if err != nil {
		return err
	}
	return resp.Error
}

func (t *Transaction) SendOffsets(ctx context.Context, offsets map[string][]TxnOffsetCommit, groupID string) error {
	// Two steps here
	// 1. AddOffsetsToTxn
	// 2. TxnOffsetCommit
	return nil
}
