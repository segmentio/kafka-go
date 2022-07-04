package kafka

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestFindCoordinatorResponseV0(t *testing.T) {
	item := findCoordinatorResponseV0{
		ErrorCode: 2,
		Coordinator: findCoordinatorResponseCoordinatorV0{
			NodeID: 3,
			Host:   "b",
			Port:   4,
		},
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found findCoordinatorResponseV0
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

func TestClientFindCoordinator(t *testing.T) {
	client, shutdown := newLocalClient()
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := waitForCoordinatorIndefinitely(ctx, client, &FindCoordinatorRequest{
		Addr:    client.Addr,
		Key:     "TransactionalID-1",
		KeyType: CoordinatorKeyTypeTransaction,
	})
	if err != nil {
		t.Fatal(err)
	}

	if resp.Coordinator.Host != "localhost" {
		t.Fatal("Coordinator should be found @ localhost")
	}
}

// WaitForCoordinatorIndefinitely is a blocking call till a coordinator is found.
func waitForCoordinatorIndefinitely(ctx context.Context, c *Client, req *FindCoordinatorRequest) (*FindCoordinatorResponse, error) {
	resp, err := c.FindCoordinator(ctx, req)

	for shouldRetryfindingCoordinator(resp, err) && ctx.Err() == nil {
		time.Sleep(1 * time.Second)
		resp, err = c.FindCoordinator(ctx, req)
	}
	return resp, err
}

// Should retry looking for coordinator
// Returns true when the test Kafka broker is still setting up.
func shouldRetryfindingCoordinator(resp *FindCoordinatorResponse, err error) bool {
	brokerSetupIncomplete := err != nil &&
		strings.Contains(
			strings.ToLower(err.Error()),
			strings.ToLower("unexpected EOF"))
	coordinatorNotFound := resp != nil &&
		resp.Error != nil &&
		errors.Is(resp.Error, GroupCoordinatorNotAvailable)
	return brokerSetupIncomplete || coordinatorNotFound
}
