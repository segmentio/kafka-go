package kafka

import (
	"sort"
	"testing"
	"time"
)

func TestOffsetStore(t *testing.T) {
	testOffsetStore(t, func() (OffsetStore, func()) {
		return NewOffsetStore(), func() {}
	})
}

func TestOffsetLog(t *testing.T) {
	testOffsetStore(t, func() (OffsetStore, func()) {
		c, err := Dial("tcp", "localhost:9092")
		if err != nil {
			panic(err)
		}

		topic := makeTopic()

		if err := c.CreateTopics(TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}); err != nil {
			panic(err)
		}

		time.Sleep(100 * time.Millisecond)

		log := &OffsetLog{
			Brokers:   []string{"localhost:9092"},
			Topic:     topic,
			Partition: 0,
		}

		return log, func() { c.DeleteTopics(topic) }
	})
}

func testOffsetStore(t *testing.T, newOffsetStore func() (store OffsetStore, close func())) {
	tests := []struct {
		scenario string
		function func(*testing.T, OffsetStore, time.Time)
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

func testOffsetStoreNewEmpty(t *testing.T, store OffsetStore, now time.Time) {
	assertOffsetStoreEmpty(t, store)
}

func testOffsetStoreDeleteEmpty(t *testing.T, store OffsetStore, now time.Time) {
	assertOffsetStoreDelete(t, store,
		Offset{Value: 1, Time: now.Add(1 * time.Second)},
		Offset{Value: 2, Time: now.Add(2 * time.Second)},
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
	)
}

func testOffsetStoreWriteEmpty(t *testing.T, store OffsetStore, now time.Time) {
	assertOffsetStoreWrite(t, store,
		Offset{Value: 1, Time: now.Add(1 * time.Second)},
		Offset{Value: 2, Time: now.Add(2 * time.Second)},
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
	)
}

func testOffsetStoreDeleteIdempotent(t *testing.T, store OffsetStore, now time.Time) {
	assertOffsetStoreWrite(t, store,
		Offset{Value: 1, Time: now.Add(1 * time.Second)},
		Offset{Value: 2, Time: now.Add(2 * time.Second)},
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
	)

	assertOffsetStoreDelete(t, store,
		Offset{Value: 1, Time: now.Add(1 * time.Second)},
	)

	assertOffsetStoreDelete(t, store,
		Offset{Value: 1, Time: now.Add(1 * time.Second)},
	)
}

func testOffsetStoreWriteIdempotent(t *testing.T, store OffsetStore, now time.Time) {
	assertOffsetStoreWrite(t, store,
		Offset{Value: 1, Time: now.Add(1 * time.Second)},
		Offset{Value: 2, Time: now.Add(2 * time.Second)},
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
	)

	assertOffsetStoreWrite(t, store,
		Offset{Value: 1, Time: now.Add(1 * time.Second)},
	)

	assertOffsetStoreWrite(t, store,
		Offset{Value: 2, Time: now.Add(2 * time.Second)},
	)

	assertOffsetStoreWrite(t, store,
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
	)
}

func testOffsetStoreDeletedOffsetsAreGone(t *testing.T, store OffsetStore, now time.Time) {
	assertOffsetStoreWrite(t, store,
		Offset{Value: 1, Time: now.Add(1 * time.Second)},
		Offset{Value: 2, Time: now.Add(2 * time.Second)},
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
	)

	assertOffsetStoreDelete(t, store,
		Offset{Value: 1},
	)

	assertOffsetStoreContains(t, store,
		Offset{Value: 2, Time: now.Add(2 * time.Second)},
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
	)

	assertOffsetStoreDelete(t, store,
		Offset{Value: 2},
	)

	assertOffsetStoreContains(t, store,
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
	)

	assertOffsetStoreDelete(t, store,
		Offset{Value: 3},
	)

	assertOffsetStoreEmpty(t, store)
}

func testOffsetStoreWrittenOffsetsExist(t *testing.T, store OffsetStore, now time.Time) {
	assertOffsetStoreWrite(t, store,
		Offset{Value: 1, Time: now.Add(1 * time.Second)},
		Offset{Value: 2, Time: now.Add(2 * time.Second)},
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
		Offset{Value: 4, Time: now.Add(4 * time.Second)},
		Offset{Value: 5, Time: now.Add(5 * time.Second)},
		Offset{Value: 6, Time: now.Add(6 * time.Second)},
		Offset{Value: 7, Time: now.Add(7 * time.Second)},
		Offset{Value: 8, Time: now.Add(8 * time.Second)},
		Offset{Value: 9, Time: now.Add(9 * time.Second)},
	)

	assertOffsetStoreContains(t, store,
		Offset{Value: 1, Time: now.Add(1 * time.Second)},
		Offset{Value: 2, Time: now.Add(2 * time.Second)},
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
		Offset{Value: 4, Time: now.Add(4 * time.Second)},
		Offset{Value: 5, Time: now.Add(5 * time.Second)},
		Offset{Value: 6, Time: now.Add(6 * time.Second)},
		Offset{Value: 7, Time: now.Add(7 * time.Second)},
		Offset{Value: 8, Time: now.Add(8 * time.Second)},
		Offset{Value: 9, Time: now.Add(9 * time.Second)},
	)
}

func testOffsetStoreWrittenOffsetsAreUnique(t *testing.T, store OffsetStore, now time.Time) {
	assertOffsetStoreWrite(t, store,
		Offset{Value: 1, Time: now.Add(1 * time.Second)},
		Offset{Value: 2, Time: now.Add(2 * time.Second)},
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
	)

	assertOffsetStoreContains(t, store,
		Offset{Value: 1, Time: now.Add(1 * time.Second)},
		Offset{Value: 2, Time: now.Add(2 * time.Second)},
		Offset{Value: 3, Time: now.Add(3 * time.Second)},
	)
}

func assertOffsetStoreWrite(t *testing.T, store OffsetStore, offsets ...Offset) {
	t.Helper()

	if err := store.WriteOffsets(offsets...); err != nil {
		t.Error("error writing offsets:", err)
	}
}

func assertOffsetStoreDelete(t *testing.T, store OffsetStore, offsets ...Offset) {
	t.Helper()

	if err := store.DeleteOffsets(offsets...); err != nil {
		t.Error("error deleting offsets:", err)
	}
}

func assertOffsetStoreEmpty(t *testing.T, store OffsetStore) {
	t.Helper()
	assertOffsetStoreContains(t, store /* nothing */)
}

func assertOffsetStoreContains(t *testing.T, store OffsetStore, offsets ...Offset) {
	t.Helper()

	var it = store.ReadOffsets()
	var found []Offset

	for it.Next() {
		found = append(found, it.Offset())
	}

	if err := it.Err(); err != nil {
		t.Error("error reading offsets:", err)
	}

	sort.Slice(found, func(i, j int) bool {
		return found[i].Value < found[j].Value
	})

	for i, off := range found {
		if i < len(offsets) {
			if off.Value != offsets[i].Value || !off.Time.Equal(offsets[i].Time) {
				t.Errorf("offset at index %d mismatch: expected %v but found %v", i, offsets[i], off)
			}
		} else {
			t.Errorf("too many offsets were found in the store: %d > %d %s", i+1, len(offsets), off)
		}
	}

	if len(found) < len(offsets) {
		t.Errorf("not enough offsets were found in the store: %d < %d", len(found), len(offsets))
	}
}
