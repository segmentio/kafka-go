package kafka

import (
	"testing"
	"time"
)

func TestOffsetScheduler(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			scenario: "stopping an offset scheduler unblocks waiting for the next offset",
			function: testOffsetSchedulerStopUnblocksNext,
		},

		{
			scenario: "offset schedules are spread within the time period of the scheduler",
			function: testOffsetSchedulerSpeadOffsetSchedules,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, test.function)
	}
}

func testOffsetSchedulerStopUnblocksNext(t *testing.T) {
	sched := newOffsetScheduler(time.Millisecond)

	time.AfterFunc(10*time.Millisecond, func() {
		sched.Stop()
	})

	before := time.Now()
	_, ok := <-sched.Offsets()
	after := time.Now()

	if ok {
		t.Error("unexpected offset returned by a call to Next")
	}

	if d := after.Sub(before); d < (10 * time.Millisecond) {
		t.Error("the call to Next returned too early:", d, "< 10ms")
	}
}

func testOffsetSchedulerSpeadOffsetSchedules(t *testing.T) {
	now := time.Now()

	sched := newOffsetScheduler(10 * time.Millisecond)
	defer sched.Stop()

	sched.Schedule(
		Offset{Value: 1, Time: now.Add(1 * time.Millisecond)},
		Offset{Value: 2, Time: now.Add(2 * time.Millisecond)},
		Offset{Value: 3, Time: now.Add(3 * time.Millisecond)},
	)

	if n := sched.Len(); n != 3 {
		t.Error("wrong number of offsets managed by the scheduler: expected 3 but found", n)
	}

	expectedOffset := Offset{
		Value: 1,
		Time:  now,
	}

	for off := range sched.Offsets() {
		t.Log(time.Now(), off)

		if off.Value != expectedOffset.Value {
			t.Errorf("scheduled offset mismatch: expected %v but found %v", expectedOffset.Value, off.Value)
		}

		if scheduledAt := time.Now(); scheduledAt.Before(expectedOffset.Time) {
			t.Errorf("offset %v has been scheduled too early: %s < %s", off, scheduledAt, expectedOffset.Time)
		}

		expectedOffset = Offset{
			Value: expectedOffset.Value + 1,
			Time:  expectedOffset.Time.Add(time.Millisecond / 3),
		}

		if expectedOffset.Value > 3 {
			sched.Stop()
		}
	}

	if expectedOffset.Value != 4 {
		t.Error("wrong number of offsets scheduled: expected 3 but found", expectedOffset.Value-1)
	}
}
