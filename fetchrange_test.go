package kafka

import "testing"

func TestMakeFetchRanges(t *testing.T) {
	tests := []struct {
		scenario string
		unused   float64
		offsets  []offset
		ranges   []fetchRange
	}{
		{
			scenario: "an empty offset list results in an empty range list",
			unused:   0.25,
			offsets:  nil,
			ranges:   nil,
		},

		{
			scenario: "contiguous offsets are collapsed in a single fetch range",
			unused:   0.25,
			offsets: []offset{
				{value: 1, size: 32},
				{value: 2, size: 64},
				{value: 3, size: 128},
			},
			ranges: []fetchRange{
				{startOffset: 1, endOffset: 4, minBytes: 224, maxBytes: 224},
			},
		},

		{
			scenario: "when tolerating no unused bytes each non-contiguous offset is placed in its own range",
			unused:   0.0,
			offsets: []offset{
				{value: 5, size: 10},
				{value: 7, size: 11},
				{value: 9, size: 12},
				{value: 1, size: 32},
				{value: 2, size: 64},
				{value: 3, size: 128},
			},
			ranges: []fetchRange{
				{startOffset: 1, endOffset: 4, minBytes: 224, maxBytes: 224},
				{startOffset: 5, endOffset: 6, minBytes: 10, maxBytes: 10},
				{startOffset: 7, endOffset: 8, minBytes: 11, maxBytes: 11},
				{startOffset: 9, endOffset: 10, minBytes: 12, maxBytes: 12},
			},
		},

		{
			scenario: "when tolerating unused bytes some offsets are merged into the same range",
			unused:   0.2,
			offsets: []offset{
				{value: 5, size: 10},
				{value: 7, size: 11},
				{value: 9, size: 12},
				{value: 1, size: 32},
				{value: 2, size: 64},
				{value: 3, size: 128},
			},
			ranges: []fetchRange{
				{startOffset: 1, endOffset: 4, minBytes: 224, maxBytes: 224},
				{startOffset: 5, endOffset: 10, minBytes: 33, maxBytes: 117},
			},
		},

		{
			scenario: "when tolerating all unused bytes all offsets are placed in the same range",
			unused:   1.0,
			offsets: []offset{
				{value: 5, size: 10},
				{value: 7, size: 11},
				{value: 9, size: 12},
				{value: 1, size: 32},
				{value: 2, size: 64},
				{value: 3, size: 128},
			},
			ranges: []fetchRange{
				{startOffset: 1, endOffset: 10, minBytes: 257, maxBytes: 383},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			testMakeFetchRange(t, test.unused, test.offsets, test.ranges)
		})
	}
}

func testMakeFetchRange(t *testing.T, unused float64, offsets []offset, ranges []fetchRange) {
	result := makeFetchRanges(unused, offsets...)

	if !fetchRangesEqual(ranges, result) {
		t.Error("ranges mismatch")

		t.Log("expected:")
		for _, r := range ranges {
			t.Logf("- %+v", r)
		}

		t.Log("found:")
		for _, r := range result {
			t.Logf("- %+v", r)
		}
	}
}

func fetchRangesEqual(r1, r2 []fetchRange) bool {
	if len(r1) != len(r2) {
		return false
	}

	for i := range r1 {
		if r1[i] != r2[i] {
			return false
		}
	}

	return true
}
