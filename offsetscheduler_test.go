package kafka

import (
	"io"
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
			scenario: "offset existing in the store before the scheduler was created are properly scheduled",
			function: testOffsetSchedulerScheduleOffsetsInStore,
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
	sched := NewOffsetScheduler(time.Millisecond, nil)

	time.AfterFunc(10*time.Millisecond, func() {
		sched.Stop()
	})

	before := time.Now()
	_, err := sched.Next()
	after := time.Now()

	if err != io.EOF {
		t.Error("wrong error returned by the call to Next: expected EOF but got", err)
	}

	if d := after.Sub(before); d < (10 * time.Millisecond) {
		t.Error("the call to Next returned too early:", d, "< 10ms")
	}
}

func testOffsetSchedulerScheduleOffsetsInStore(t *testing.T) {
	now := time.Now()

	store := NewOffsetStore()
	store.WriteOffsets(
		Offset{Value: 1, Time: now.Add(1 * time.Millisecond)},
		Offset{Value: 2, Time: now.Add(2 * time.Millisecond)},
		Offset{Value: 3, Time: now.Add(3 * time.Millisecond)},
	)

	sched := NewOffsetScheduler(time.Millisecond, store)
	defer sched.Stop()

	if err := sched.Sync(); err != nil {
		t.Error("error syncing scheduler:", err)
	}

	expectedOffset := Offset{
		Value: 1,
		Time:  now.Add(time.Millisecond),
	}

	for i := 0; i < 3; i++ {
		off, err := sched.Next()

		if err != nil {
			break
		}

		if off.Value != expectedOffset.Value {
			t.Errorf("scheduled offset mismatch: expected %v but found %v", expectedOffset.Value, off.Value)
		}

		if scheduledAt := time.Now(); scheduledAt.Before(expectedOffset.Time) {
			t.Errorf("offset %v has been scheduled too early: %s < %s", off, scheduledAt, expectedOffset.Time)
		}

		expectedOffset = Offset{
			Value: expectedOffset.Value + 1,
			Time:  expectedOffset.Time.Add(time.Millisecond),
		}
	}
}

func testOffsetSchedulerSpeadOffsetSchedules(t *testing.T) {
	now := time.Now()

	sched := NewOffsetScheduler(time.Millisecond, nil)
	defer sched.Stop()

	sched.Schedule(
		Offset{Value: 1, Time: now.Add(1 * time.Microsecond)},
		Offset{Value: 2, Time: now.Add(2 * time.Microsecond)},
		Offset{Value: 3, Time: now.Add(3 * time.Microsecond)},
	)

	expectedOffset := Offset{
		Value: 1,
		Time:  now,
	}

	for i := 0; i < 3; i++ {
		off, err := sched.Next()

		if err != nil {
			break
		}

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
	}

}
