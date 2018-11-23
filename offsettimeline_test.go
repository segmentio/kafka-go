package kafka

import (
	"testing"
	"time"
)

func TestOffsetTimeline(t *testing.T) {
	tests := []struct {
		scenorio string
		function func(*testing.T)
	}{
		{
			scenorio: "an empty offset timeline has a length of zero",
			function: testOffsetTimelineEmpty,
		},

		{
			scenorio: "the length of an offset timeline changes when offsets are pushed",
			function: testOffsetTimelineLengthAfterPush,
		},

		{
			scenorio: "the length of an offset timeline changes when offsets are popped",
			function: testOffsetTimelineLengthAfterPop,
		},

		{
			scenorio: "offsets pushed to an offset timeline are popped ordered by time",
			function: testOffsetTimelinePushAndPop,
		},
	}

	for _, test := range tests {
		t.Run(test.scenorio, test.function)
	}
}

func testOffsetTimelineEmpty(t *testing.T) {
	timeline := offsetTimeline{}

	if n := timeline.len(); n != 0 {
		t.Error("empty offset timeline has a non-zero length:", n)
	}
}

func testOffsetTimelineLengthAfterPush(t *testing.T) {
	now := time.Now()

	timeline := offsetTimeline{}
	timeline.push(
		offset{value: 1, time: utime(now, 1*time.Second)},
		offset{value: 2, time: utime(now, 2*time.Second)},
		offset{value: 3, time: utime(now, 3*time.Second)},
	)

	if n := timeline.len(); n != 3 {
		t.Error("offset timeline length mismatch: expected 3 but found", n)
	}
}

func testOffsetTimelineLengthAfterPop(t *testing.T) {
	now := time.Now()

	timeline := offsetTimeline{}
	timeline.push(
		offset{value: 1, time: utime(now, 1*time.Second)},
		offset{value: 2, time: utime(now, 2*time.Second)},
		offset{value: 3, time: utime(now, 3*time.Second)},
	)

	timeline.pop(now.Add(2 * time.Second))

	if n := timeline.len(); n != 1 {
		t.Error("offset timeline length mismatch: expected 1 but found", n)
	}
}

func testOffsetTimelinePushAndPop(t *testing.T) {
	now := time.Now()

	offset1 := offset{value: 1, time: utime(now, 1*time.Second)}
	offset2 := offset{value: 2, time: utime(now, 2*time.Second)}
	offset3 := offset{value: 3, time: utime(now, 3*time.Second)}

	timeline := offsetTimeline{}
	timeline.push(offset3, offset2, offset1)

	offsets := timeline.pop(now.Add(2 * time.Second))

	switch {
	case len(offsets) != 2:
		t.Error("offset timeline popped the wrong number of offsets: expected 2 but found", len(offsets))

	case offsets[0] != offset1:
		t.Errorf("offset at index 0 mismatch: expected %v but found %v", offset1, offsets[0])

	case offsets[1] != offset2:
		t.Errorf("offset at index 1 mismatch: expected %v but found %v", offset2, offsets[1])
	}
}

func utime(t time.Time, d time.Duration) int64 {
	return t.Add(d).UnixNano()
}
