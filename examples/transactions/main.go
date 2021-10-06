package main

import (
	"context"

	"github.com/segmentio/kafka-go"
)

func main() {
	groupID := "group-id"
	writer := kafka.Writer{}
	reader := kafka.NewReader(kafka.ReaderConfig{
		GroupID:        groupID,
		IsolationLevel: kafka.ReadCommitted,
	})

	writer.InitTransactions(context.TODO())

	for {
		txn, _ := writer.BeginTxn()
		msg, _ := reader.FetchMessage(context.TODO())

		txn.WriteMessages(context.TODO(), kafka.Message{}, kafka.Message{})

		txn.SendOffsets(context.TODO(), map[string][]kafka.TxnOffsetCommit{
			msg.Topic: {
				{
					Partition: msg.Partition,
					Offset:    msg.Offset,
				},
			},
		}, groupID)

		txn.Commit(context.TODO())
	}
}
