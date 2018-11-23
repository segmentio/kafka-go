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
		sched.stop()
	})

	before := time.Now()
	offsets, ok := <-sched.offsets()
	after := time.Now()

	if ok {
		t.Error("unexpected offset returned by a call to offsets:", offsets)
	}

	if d := after.Sub(before); d < (10 * time.Millisecond) {
		t.Error("the call to Next returned too early:", d, "< 10ms")
	}
}

func testOffsetSchedulerSpeadOffsetSchedules(t *testing.T) {
	now := time.Now()

	sched := newOffsetScheduler(10 * time.Millisecond)
	defer sched.stop()

	sched.schedule(
		offset{value: 1, time: utime(now, 1*time.Millisecond)},
		offset{value: 2, time: utime(now, 2*time.Millisecond)},
		offset{value: 3, time: utime(now, 3*time.Millisecond)},
	)

	if n := sched.len(); n != 3 {
		t.Error("wrong number of offsets managed by the scheduler: expected 3 but found", n)
	}

	expectedOffset := offset{
		value: 1,
		time:  now.UnixNano(),
	}

	foundOffsets := <-sched.offsets()

	for _, off := range foundOffsets {
		t.Log(time.Now(), off)

		if off.value != expectedOffset.value {
			t.Errorf("scheduled offset mismatch: expected %v but found %v", expectedOffset.value, off.value)
		}

		if scheduledAt, expectedScheduledAt := time.Now(), time.Unix(0, expectedOffset.time); scheduledAt.Before(expectedScheduledAt) {
			t.Errorf("offset %v has been scheduled too early: %s < %s", off, scheduledAt, expectedScheduledAt)
		}

		expectedOffset = offset{
			value: expectedOffset.value + 1,
			time:  expectedOffset.time + int64(time.Millisecond/3),
		}
	}

	if len(foundOffsets) != 3 {
		t.Error("wrong number of offsets scheduled: expected 3 but found", len(foundOffsets))
	}
}
