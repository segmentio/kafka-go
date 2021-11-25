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

	topicPartitions map[topicPartition]struct{}
}

func (t *Transaction) WriteMessages(ctx context.Context, msgs ...Message) error {
	assignments, err := t.w.generateAssignments(ctx, msgs...)
	if err != nil {
		return err
	}

	topicPartitionsToAdd := map[string][]AddPartitionToTxn{}

	for tp := range assignments {
		if _, ok := t.topicPartitions[tp]; !ok {
			topicPartitionsToAdd[tp.topic] = append(topicPartitionsToAdd[tp.topic], AddPartitionToTxn{Partition: int(tp.partition)})
		}
	}

	resp, err := t.w.client(t.w.writeTimeout()).AddPartitionsToTxn(ctx, &AddPartitionsToTxnRequest{
		TransactionalID: t.transactionalID,
		ProducerID:      t.producerID,
		ProducerEpoch:   t.producerEpoch,
		Topics:          topicPartitionsToAdd,
	})
	if err != nil {
		return err
	}

	for _, parts := range resp.Topics {
		for _, part := range parts {
			if part.Error != nil {
				return part.Error
			}
		}
	}

	return t.w.writeMessages(ctx, assignments, msgs...)
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

func (t *Transaction) SendOffsets(ctx context.Context, offsets map[string][]TxnOffsetCommit, generation *Generation) error {
	resp, err := t.w.client(t.w.writeTimeout()).AddOffsetsToTxn(ctx, &AddOffsetsToTxnRequest{
		TransactionalID: t.transactionalID,
		ProducerID:      t.producerID,
		ProducerEpoch:   t.producerEpoch,
		GroupID:         generation.GroupID,
	})
	if err != nil {
		return err
	}

	if resp.Error != nil {
		return resp.Error
	}

	offsetResp, err := t.w.client(t.w.writeTimeout()).TxnOffsetCommit(ctx, &TxnOffsetCommitRequest{
		Addr:            nil,
		TransactionalID: t.transactionalID,
		GroupID:         generation.GroupID,
		ProducerID:      t.producerID,
		ProducerEpoch:   t.producerEpoch,
		GenerationID:    int(generation.ID),
		MemberID:        generation.MemberID,
		Topics:          offsets,
	})
	if err != nil {
		return err
	}

	for _, resps := range offsetResp.Topics {
		for _, resp := range resps {
			if resp.Error != nil {
				return resp.Error
			}
		}
	}

	return nil
}
