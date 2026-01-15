package kafka

import (
	"context"
	"testing"
	"time"
)

func TestDelay_GetBlocksUntilDeliver(t *testing.T) {
	d := NewDelay[int]()

	done := make(chan int)
	go func() {
		v, ok := d.Get(context.Background())
		if ok {
			done <- v
		}
	}()

	// Should not receive yet
	select {
	case <-done:
		t.Fatal("Get returned before Deliver")
	case <-time.After(50 * time.Millisecond):
		// expected
	}

	d.Deliver(42)

	select {
	case v := <-done:
		if v != 42 {
			t.Fatalf("expected 42, got %d", v)
		}
	case <-time.After(time.Second):
		t.Fatal("Get did not return after Deliver")
	}
}

func TestDelay_GetIfDelivered(t *testing.T) {
	d := NewDelay[string]()

	// Before deliver
	_, ok := d.GetIfDelivered()
	if ok {
		t.Fatal("GetIfDelivered returned true before Deliver")
	}

	d.Deliver("hello")

	// After deliver
	v, ok := d.GetIfDelivered()
	if !ok {
		t.Fatal("GetIfDelivered returned false after Deliver")
	}
	if v != "hello" {
		t.Fatalf("expected 'hello', got '%s'", v)
	}
}

func TestDelay_ContextCancellation(t *testing.T) {
	d := NewDelay[int]()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool)
	go func() {
		_, ok := d.Get(ctx)
		done <- ok
	}()

	// Give the goroutine time to start blocking
	time.Sleep(10 * time.Millisecond)

	cancel()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("Get returned true on cancelled context")
		}
	case <-time.After(time.Second):
		t.Fatal("Get did not return after context cancellation")
	}
}

func TestDelay_MultipleWaiters(t *testing.T) {
	d := NewDelay[int]()

	results := make(chan int, 3)
	for i := 0; i < 3; i++ {
		go func() {
			v, ok := d.Get(context.Background())
			if ok {
				results <- v
			} else {
				results <- -1
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	d.Deliver(99)

	for i := 0; i < 3; i++ {
		select {
		case v := <-results:
			if v != 99 {
				t.Fatalf("expected 99, got %d", v)
			}
		case <-time.After(time.Second):
			t.Fatal("waiter did not return")
		}
	}
}

func TestDelay_CancelUnblocksWaiters(t *testing.T) {
	d := NewDelay[int]()

	done := make(chan bool)
	go func() {
		_, ok := d.Get(context.Background())
		done <- ok
	}()

	// Give the goroutine time to start blocking
	time.Sleep(10 * time.Millisecond)

	d.Cancel()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("Get returned true after Cancel (should return false)")
		}
	case <-time.After(time.Second):
		t.Fatal("Get did not return after Cancel")
	}
}

func TestDelay_CancelAfterDeliverIsNoop(t *testing.T) {
	d := NewDelay[string]()

	d.Deliver("first")
	d.Cancel() // should be a no-op since Deliver already resolved

	v, ok := d.GetIfDelivered()
	if !ok || v != "first" {
		t.Fatalf("expected ('first', true), got ('%s', %v)", v, ok)
	}
}

func TestDelay_DeliverAfterCancelIsNoop(t *testing.T) {
	d := NewDelay[string]()

	d.Cancel()
	d.Deliver("value") // should be a no-op since Cancel already resolved

	_, ok := d.GetIfDelivered()
	if ok {
		t.Fatal("GetIfDelivered returned true after Cancel (Deliver should be no-op)")
	}
}

func TestDelay_GetIfDeliveredAfterCancel(t *testing.T) {
	d := NewDelay[int]()

	d.Cancel()

	_, ok := d.GetIfDelivered()
	if ok {
		t.Fatal("GetIfDelivered returned true after Cancel")
	}
}

func TestDelay_Chan(t *testing.T) {
	d := NewDelay[int]()

	// Chan should not be closed initially
	select {
	case <-d.Chan():
		t.Fatal("Chan was closed before Deliver")
	default:
		// expected
	}

	d.Deliver(42)

	// Chan should be closed after Deliver
	select {
	case <-d.Chan():
		// expected
	default:
		t.Fatal("Chan was not closed after Deliver")
	}

	// GetIfDelivered should work after selecting on Chan
	v, ok := d.GetIfDelivered()
	if !ok || v != 42 {
		t.Fatalf("expected (42, true), got (%d, %v)", v, ok)
	}
}

func TestDelay_ChanWithCancel(t *testing.T) {
	d := NewDelay[int]()

	d.Cancel()

	// Chan should be closed after Cancel
	select {
	case <-d.Chan():
		// expected
	default:
		t.Fatal("Chan was not closed after Cancel")
	}

	// GetIfDelivered should return false after Cancel
	_, ok := d.GetIfDelivered()
	if ok {
		t.Fatal("GetIfDelivered returned true after Cancel")
	}
}
