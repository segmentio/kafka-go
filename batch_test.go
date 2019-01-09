package kafka

import (
	"context"
	"io"
	"net"
	"strconv"
	"testing"
)

func TestBatchDontExpectEOF(t *testing.T) {
	topic := makeTopic()

	broker, err := (&Dialer{
		Resolver: &net.Resolver{},
	}).LookupLeader(context.Background(), "tcp", "localhost:9092", topic, 0)
	if err != nil {
		t.Fatal("failed to open a new kafka connection:", err)
	}

	nc, err := net.Dial("tcp", net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)))
	if err != nil {
		t.Fatalf("cannot connect to partition leader at %s:%d: %s", broker.Host, broker.Port, err)
	}

	conn := NewConn(nc, topic, 0)
	defer conn.Close()

	nc.(*net.TCPConn).CloseRead()

	batch := conn.ReadBatch(1024, 8192)

	if _, err := batch.ReadMessage(); err != io.ErrUnexpectedEOF {
		t.Error("bad error when reading message:", err)
	}

	if err := batch.Close(); err != io.ErrUnexpectedEOF {
		t.Error("bad error when closing the batch:", err)
	}
}
