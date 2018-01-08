package kafka

import (
	"context"
	"testing"
	"time"
)

func TestRunGroup(t *testing.T) {
	t.Run("Wait returns on empty group", func(t *testing.T) {
		rg := &runGroup{}
		rg.Wait()
	})

	t.Run("Stop returns on empty group", func(t *testing.T) {
		rg := &runGroup{}
		rg.Stop()
	})

	t.Run("Stop cancels running tasks", func(t *testing.T) {
		rg := &runGroup{}
		rg.Go(func(stop <-chan struct{}) {
			<-stop
		})
		rg.Stop()
	})

	t.Run("Honors parent context", func(t *testing.T) {
		now := time.Now()
		timeout := time.Millisecond * 100

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		rg := &runGroup{}
		rg = rg.WithContext(ctx)
		rg.Go(func(stop <-chan struct{}) {
			<-stop
		})
		rg.Wait()

		elapsed := time.Now().Sub(now)
		if elapsed < timeout {
			t.Errorf("expected elapsed > %v; got %v", timeout, elapsed)
		}
	})

	t.Run("Any death kills all; one for all and all for one", func(t *testing.T) {
		rg := &runGroup{}
		rg.Go(func(stop <-chan struct{}) {
			<-stop
		})
		rg.Go(func(stop <-chan struct{}) {
			<-stop
		})
		rg.Go(func(stop <-chan struct{}) {
			// return immediately
		})
		rg.Wait()
	})
}
