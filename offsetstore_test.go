package kafka

import (
	"sort"
	"testing"
	"time"
)

func TestOffsetStore(t *testing.T) {
	testOffsetStore(t, func() (offsetStore, func()) {
		return newOffsetStore(), func() {}
	})
}

func testOffsetStore(t *testing.T, newOffsetStore func() (store offsetStore, close func())) {
	tests := []struct {
		scenario string
		function func(*testing.T, offsetStore, time.Time)
	}{
		{
			scenario: "a newly constructed offset store does not contain any offsets",
			function: testOffsetStoreNewEmpty,
		},

		{
			scenario: "deleting offsets from an empty store succeeds",
			function: testOffsetStoreDeleteEmpty,
		},

		{
			scenario: "writing offsets to an empty store succeeds",
			function: testOffsetStoreWriteEmpty,
		},

		{
			scenario: "deleting offsets is an idempotent operation and does not fail when the offsets have already been deleted",
			function: testOffsetStoreDeleteIdempotent,
		},

		{
			scenario: "writing offsets is an idempotent operation and does not fail when the offsets have already been written",
			function: testOffsetStoreWriteIdempotent,
		},

		{
			scenario: "deleted offsets are not returned anymore when reading the content of the store",
			function: testOffsetStoreDeletedOffsetsAreGone,
		},

		{
			scenario: "offsets written are returned when reading the content of the store",
			function: testOffsetStoreWrittenOffsetsExist,
		},

		{
			scenario: "offsets written multiple times are only returned once when reading the content of the store",
			function: testOffsetStoreWrittenOffsetsAreUnique,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			store, close := newOffsetStore()
			defer close()

			testFunc(t, store, time.Now())
		})
	}
}

func testOffsetStoreNewEmpty(t *testing.T, store offsetStore, now time.Time) {
	assertOffsetStoreEmpty(t, store)
}

func testOffsetStoreDeleteEmpty(t *testing.T, store offsetStore, now time.Time) {
	assertOffsetStoreDelete(t, store,
		offset{value: 1, attempt: 3, size: 10, time: utime(now, 1*time.Second)},
		offset{value: 2, attempt: 2, size: 20, time: utime(now, 2*time.Second)},
		offset{value: 3, attempt: 2, size: 30, time: utime(now, 3*time.Second)},
	)
}

func testOffsetStoreWriteEmpty(t *testing.T, store offsetStore, now time.Time) {
	assertOffsetStoreWrite(t, store,
		offset{value: 1, attempt: 3, size: 10, time: utime(now, 1*time.Second)},
		offset{value: 2, attempt: 2, size: 20, time: utime(now, 2*time.Second)},
		offset{value: 3, attempt: 2, size: 30, time: utime(now, 3*time.Second)},
	)
}

func testOffsetStoreDeleteIdempotent(t *testing.T, store offsetStore, now time.Time) {
	assertOffsetStoreWrite(t, store,
		offset{value: 1, attempt: 3, size: 10, time: utime(now, 1*time.Second)},
		offset{value: 2, attempt: 2, size: 20, time: utime(now, 2*time.Second)},
		offset{value: 3, attempt: 2, size: 30, time: utime(now, 3*time.Second)},
	)

	assertOffsetStoreDelete(t, store,
		offset{value: 1},
	)

	assertOffsetStoreDelete(t, store,
		offset{value: 1},
	)
}

func testOffsetStoreWriteIdempotent(t *testing.T, store offsetStore, now time.Time) {
	assertOffsetStoreWrite(t, store,
		offset{value: 1, attempt: 3, size: 10, time: utime(now, 1*time.Second)},
		offset{value: 2, attempt: 2, size: 20, time: utime(now, 2*time.Second)},
		offset{value: 3, attempt: 2, size: 30, time: utime(now, 3*time.Second)},
	)

	assertOffsetStoreWrite(t, store,
		offset{value: 1, attempt: 3, size: 10, time: utime(now, 1*time.Second)},
	)

	assertOffsetStoreWrite(t, store,
		offset{value: 2, attempt: 2, size: 20, time: utime(now, 2*time.Second)},
	)

	assertOffsetStoreWrite(t, store,
		offset{value: 3, attempt: 2, size: 30, time: utime(now, 3*time.Second)},
	)
}

func testOffsetStoreDeletedOffsetsAreGone(t *testing.T, store offsetStore, now time.Time) {
	assertOffsetStoreWrite(t, store,
		offset{value: 1, attempt: 3, size: 10, time: utime(now, 1*time.Second)},
		offset{value: 2, attempt: 2, size: 20, time: utime(now, 2*time.Second)},
		offset{value: 3, attempt: 2, size: 30, time: utime(now, 3*time.Second)},
	)

	assertOffsetStoreDelete(t, store,
		offset{value: 1},
	)

	assertOffsetStoreContains(t, store,
		offset{value: 2, attempt: 2, size: 20, time: utime(now, 2*time.Second)},
		offset{value: 3, attempt: 2, size: 30, time: utime(now, 3*time.Second)},
	)

	assertOffsetStoreDelete(t, store,
		offset{value: 2},
	)

	assertOffsetStoreContains(t, store,
		offset{value: 3, attempt: 2, size: 30, time: utime(now, 3*time.Second)},
	)

	assertOffsetStoreDelete(t, store,
		offset{value: 3},
	)

	assertOffsetStoreEmpty(t, store)
}

func testOffsetStoreWrittenOffsetsExist(t *testing.T, store offsetStore, now time.Time) {
	assertOffsetStoreWrite(t, store,
		offset{value: 1, attempt: 3, size: 10, time: utime(now, 1*time.Second)},
		offset{value: 2, attempt: 3, size: 10, time: utime(now, 2*time.Second)},
		offset{value: 3, attempt: 3, size: 10, time: utime(now, 3*time.Second)},
		offset{value: 4, attempt: 3, size: 10, time: utime(now, 4*time.Second)},
		offset{value: 5, attempt: 3, size: 10, time: utime(now, 5*time.Second)},
		offset{value: 6, attempt: 3, size: 10, time: utime(now, 6*time.Second)},
		offset{value: 7, attempt: 3, size: 10, time: utime(now, 7*time.Second)},
		offset{value: 8, attempt: 3, size: 10, time: utime(now, 8*time.Second)},
		offset{value: 9, attempt: 3, size: 10, time: utime(now, 9*time.Second)},
	)

	assertOffsetStoreContains(t, store,
		offset{value: 1, attempt: 3, size: 10, time: utime(now, 1*time.Second)},
		offset{value: 2, attempt: 3, size: 10, time: utime(now, 2*time.Second)},
		offset{value: 3, attempt: 3, size: 10, time: utime(now, 3*time.Second)},
		offset{value: 4, attempt: 3, size: 10, time: utime(now, 4*time.Second)},
		offset{value: 5, attempt: 3, size: 10, time: utime(now, 5*time.Second)},
		offset{value: 6, attempt: 3, size: 10, time: utime(now, 6*time.Second)},
		offset{value: 7, attempt: 3, size: 10, time: utime(now, 7*time.Second)},
		offset{value: 8, attempt: 3, size: 10, time: utime(now, 8*time.Second)},
		offset{value: 9, attempt: 3, size: 10, time: utime(now, 9*time.Second)},
	)
}

func testOffsetStoreWrittenOffsetsAreUnique(t *testing.T, store offsetStore, now time.Time) {
	assertOffsetStoreWrite(t, store,
		offset{value: 1, attempt: 3, size: 10, time: utime(now, 1*time.Second)},
		offset{value: 2, attempt: 3, size: 10, time: utime(now, 2*time.Second)},
		offset{value: 3, attempt: 3, size: 10, time: utime(now, 3*time.Second)},
		offset{value: 3, attempt: 3, size: 10, time: utime(now, 3*time.Second)},
		offset{value: 3, attempt: 3, size: 10, time: utime(now, 3*time.Second)},
	)

	assertOffsetStoreContains(t, store,
		offset{value: 1, attempt: 3, size: 10, time: utime(now, 1*time.Second)},
		offset{value: 2, attempt: 3, size: 10, time: utime(now, 2*time.Second)},
		offset{value: 3, attempt: 3, size: 10, time: utime(now, 3*time.Second)},
	)
}

func assertOffsetStoreWrite(t *testing.T, store offsetStore, offsets ...offset) {
	t.Helper()

	if err := store.writeOffsets(offsets...); err != nil {
		t.Error("error writing offsets:", err)
	}
}

func assertOffsetStoreDelete(t *testing.T, store offsetStore, offsets ...offset) {
	t.Helper()

	if err := store.deleteOffsets(offsets...); err != nil {
		t.Error("error deleting offsets:", err)
	}
}

func assertOffsetStoreEmpty(t *testing.T, store offsetStore) {
	t.Helper()
	assertOffsetStoreContains(t, store /* nothing */)
}

func assertOffsetStoreContains(t *testing.T, store offsetStore, offsets ...offset) {
	t.Helper()

	found, err := store.readOffsets()
	if err != nil {
		t.Error("error reading offsets:", err)
	}

	sort.Slice(found, func(i, j int) bool {
		return found[i].value < found[j].value
	})

	for i, off := range found {
		if i < len(offsets) {
			if off != offsets[i] {
				t.Errorf("offset at index %d mismatch: expected %+v but found %+v", i, offsets[i], off)
			}
		} else {
			t.Errorf("too many offsets were found in the store: %d > %d %+v", i+1, len(offsets), off)
		}
	}

	if len(found) < len(offsets) {
		t.Errorf("not enough offsets were found in the store: %d < %d", len(found), len(offsets))
	}
}
