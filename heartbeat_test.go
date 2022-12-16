package kafka

import (
	"bufio"
	"bytes"
	"context"
	"log"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestClientHeartbeat(t *testing.T) {
	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	groupID := makeGroupID()

	group, err := NewConsumerGroup(ConsumerGroupConfig{
		ID:                groupID,
		Topics:            []string{topic},
		Brokers:           []string{"localhost:9092"},
		HeartbeatInterval: 2 * time.Second,
		RebalanceTimeout:  2 * time.Second,
		RetentionTime:     time.Hour,
		Logger:            log.New(os.Stdout, "cg-test: ", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()

	gen, err := group.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	resp, err := client.Heartbeat(ctx, &HeartbeatRequest{
		GroupID:      groupID,
		GenerationID: gen.ID,
		MemberID:     gen.MemberID,
	})
	if err != nil {
		t.Fatal(err)
	}

	if resp.Error != nil {
		t.Error(resp.Error)
	}
}

func TestHeartbeatRequestV0(t *testing.T) {
	item := heartbeatResponseV0{
		ErrorCode: 2,
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found heartbeatResponseV0
	remain, err := (&found).readFrom(bufio.NewReader(b), b.Len())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if remain != 0 {
		t.Errorf("expected 0 remain, got %v", remain)
		t.FailNow()
	}
	if !reflect.DeepEqual(item, found) {
		t.Error("expected item and found to be the same")
		t.FailNow()
	}
}
