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
	timeline := OffsetTimeline{}

	if n := timeline.Len(); n != 0 {
		t.Error("empty offset timeline has a non-zero length:", n)
	}
}

func testOffsetTimelineLengthAfterPush(t *testing.T) {
	now := time.Now()

	timeline := OffsetTimeline{}
	timeline.Push(
		Offset{Value: 1, Time: now.Add(1 * time.Second)},
		Offset{Value: 2, Time: now.Add(2 * time.Second)},
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
	)

	if n := timeline.Len(); n != 3 {
		t.Error("offset timeline length mismatch: expected 3 but found", n)
	}
}

func testOffsetTimelineLengthAfterPop(t *testing.T) {
	now := time.Now()

	timeline := OffsetTimeline{}
	timeline.Push(
		Offset{Value: 1, Time: now.Add(1 * time.Second)},
		Offset{Value: 2, Time: now.Add(2 * time.Second)},
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
	)

	timeline.Pop(now.Add(2 * time.Second))

	if n := timeline.Len(); n != 1 {
		t.Error("offset timeline length mismatch: expected 1 but found", n)
	}
}

func testOffsetTimelinePushAndPop(t *testing.T) {
	now := time.Now()

	offset1 := Offset{Value: 1, Time: now.Add(1 * time.Second)}
	offset2 := Offset{Value: 2, Time: now.Add(2 * time.Second)}
	offset3 := Offset{Value: 3, Time: now.Add(3 * time.Second)}

	timeline := OffsetTimeline{}
	timeline.Push(offset3, offset2, offset1)

	offsets := timeline.Pop(now.Add(2 * time.Second))

	switch {
	case len(offsets) != 2:
		t.Error("offset timeline popped the wrong number of offsets: expected 2 but found", len(offsets))

	case !offsets[0].Equal(offset1):
		t.Errorf("offset at index 0 mismatch: expected %v but found %v", offset1, offsets[0])

	case !offsets[1].Equal(offset2):
		t.Errorf("offset at index 1 mismatch: expected %v but found %v", offset2, offsets[1])
	}
}
