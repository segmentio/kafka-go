package kafka

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestConn(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T, *Conn)
	}{
		{
			scenario: "close right away",
			function: testConnClose,
		},

		{
			scenario: "write messages to kafka",
			function: testConnWrite,
		},
	}

	id := int32(0)

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			topic := fmt.Sprintf("kafka-go-%02d", atomic.AddInt32(&id, 1))

			conn, err := (&Dialer{
				Resolver: &net.Resolver{PreferGo: true},
			}).DialLeader(ctx, "tcp", "localhost:9092", topic, 0)
			if err != nil {
				t.Fatal("failed to open a new kafka connection:", err)
			}
			defer conn.Close()
			testFunc(t, conn)
		})
	}
}

func testConnClose(t *testing.T, conn *Conn) {
	if err := conn.Close(); err != nil {
		t.Error(err)
	}
}

func testConnWrite(t *testing.T, conn *Conn) {
	b := []byte("Hello World!")
	n, err := conn.Write(b)

	if err != nil {
		t.Error(err)
	}

	if n != len(b) {
		t.Error("bad length returned by (*Conn).Write:", n)
	}
}
