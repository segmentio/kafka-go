package records_test

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go/records"
)

func TestIndex(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		testIndex(t, func() (records.Index, func(), error) {
			return records.NewIndex(), func() {}, nil
		})
	})
}

func testIndex(t *testing.T, newIndex func() (records.Index, func(), error)) {
	tests := []struct {
		scenario string
		function func(*testing.T, records.Index)
	}{
		{
			scenario: "newly created indexes contain no entries",
			function: testIndexEmpty,
		},

		{
			scenario: "keys inserted in the index are exposed by selections",
			function: testIndexInsertAndSelect,
		},

		{
			scenario: "keys deleted are not exposed by selections",
			function: testIndexInsertAndDelete,
		},

		{
			scenario: "keys matching dropped prefixes are not exposed by selections",
			function: testIndexInsertAndDropKeys,
		},

		{
			scenario: "keys matching dropped offsets are not exposed by selections",
			function: testIndexInsertAndDropOffsets,
		},

		{
			scenario: "keys can be deleted in the same update where they are inserted",
			function: testIndexInsertAndDeleteInUpdate,
		},

		{
			scenario: "keys can be dropped by prefix in the same update where they are inserted",
			function: testIndexInsertAndDropKeysInUpdate,
		},

		{
			scenario: "keys can be dropped by offset in the same update where they are inserted",
			function: testIndexInsertAndDropOffsetsInUpdate,
		},

		{
			scenario: "not comitting updates does not mutate the index",
			function: testIndexUpdateNoCommit,
		},

		{
			scenario: "updates are visible in the order in which they are committed",
			function: testIndexUpdateOrder,
		},

		{
			scenario: "selections only exposes the keys matching a given prefix",
			function: testIndexSelectPrefix,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			index, teardown, err := newIndex()
			if err != nil {
				t.Fatal(err)
			}
			defer teardown()
			test.function(t, index)
		})
	}
}

func testIndexEmpty(t *testing.T, index records.Index) {
	assertIndexState(t, index, nil)
}

func testIndexInsertAndSelect(t *testing.T, index records.Index) {
	update(t, index, func(u records.Update) {
		u.Insert([]byte("A"), 1)
		u.Insert([]byte("B"), 2)
		u.Insert([]byte("C"), 3)
	})
	assertIndexState(t, index, indexState{{"A", 1}, {"B", 2}, {"C", 3}})
}

func testIndexInsertAndDelete(t *testing.T, index records.Index) {
	update(t, index, func(u records.Update) {
		u.Insert([]byte("A"), 1)
		u.Insert([]byte("B"), 2)
		u.Insert([]byte("C"), 3)
	})
	update(t, index, func(u records.Update) {
		u.Delete([]byte("A"))
	})
	assertIndexState(t, index, indexState{{"B", 2}, {"C", 3}})
}

func testIndexInsertAndDropKeys(t *testing.T, index records.Index) {
	update(t, index, func(u records.Update) {
		u.Insert([]byte("p.A"), 1)
		u.Insert([]byte("p.B"), 2)
		u.Insert([]byte("q.C"), 3)
	})
	update(t, index, func(u records.Update) {
		u.DropKeys([]byte("p."))
	})
	assertIndexState(t, index, indexState{{"q.C", 3}})
}

func testIndexInsertAndDropOffsets(t *testing.T, index records.Index) {
	update(t, index, func(u records.Update) {
		u.Insert([]byte("A"), 1)
		u.Insert([]byte("B"), 2)
		u.Insert([]byte("C"), 3)
	})
	update(t, index, func(u records.Update) {
		u.DropOffsets(3)
	})
	assertIndexState(t, index, indexState{{"C", 3}})
}

func testIndexInsertAndDeleteInUpdate(t *testing.T, index records.Index) {
	update(t, index, func(u records.Update) {
		u.Insert([]byte("A"), 1)
		u.Insert([]byte("B"), 2)
		u.Insert([]byte("C"), 3)
		u.Delete([]byte("A"))
	})
	assertIndexState(t, index, indexState{{"B", 2}, {"C", 3}})
}

func testIndexInsertAndDropKeysInUpdate(t *testing.T, index records.Index) {
	update(t, index, func(u records.Update) {
		u.Insert([]byte("p.A"), 1)
		u.Insert([]byte("p.B"), 2)
		u.Insert([]byte("q.C"), 3)
		u.DropKeys([]byte("p."))
	})
	assertIndexState(t, index, indexState{{"q.C", 3}})
}

func testIndexInsertAndDropOffsetsInUpdate(t *testing.T, index records.Index) {
	update(t, index, func(u records.Update) {
		u.Insert([]byte("A"), 1)
		u.Insert([]byte("B"), 2)
		u.Insert([]byte("C"), 3)
		u.DropOffsets(3)
	})
	assertIndexState(t, index, indexState{{"C", 3}})
}

func testIndexUpdateNoCommit(t *testing.T, index records.Index) {
	u := openTx(t, index)
	defer u.Close()
	u.Insert([]byte("A"), 1)
	u.Insert([]byte("B"), 2)
	u.Insert([]byte("C"), 3)
	assertIndexState(t, index, nil)
}

func testIndexUpdateOrder(t *testing.T, index records.Index) {
	u1 := openTx(t, index)
	defer u1.Close()
	u1.Insert([]byte("A"), 1)
	u1.Insert([]byte("B"), 2)

	u2 := openTx(t, index)
	defer u2.Close()
	u2.Insert([]byte("C"), 3)

	u2.Commit()
	assertIndexState(t, index, indexState{{"C", 3}})

	u1.Commit()
	assertIndexState(t, index, indexState{{"A", 1}, {"B", 2}, {"C", 3}})
}

func testIndexSelectPrefix(t *testing.T, index records.Index) {
	update(t, index, func(u records.Update) {
		u.Insert([]byte("A.1"), 1)
		u.Insert([]byte("A.2"), 2)
		u.Insert([]byte("A.3"), 3)

		u.Insert([]byte("B.1"), 4)
		u.Insert([]byte("B.2"), 5)

		u.Insert([]byte("C.1"), 6)
	})

	s1 := section(index, []byte("A."))
	s2 := section(index, []byte("B."))
	s3 := section(index, []byte("C."))

	assertIndexState(t, s1, indexState{{"A.1", 1}, {"A.2", 2}, {"A.3", 3}})
	assertIndexState(t, s2, indexState{{"B.1", 4}, {"B.2", 5}})
	assertIndexState(t, s3, indexState{{"C.1", 6}})
}

type indexSection struct {
	base   records.Index
	prefix []byte
}

func section(i records.Index, p []byte) records.Index {
	return &indexSection{
		base:   i,
		prefix: p,
	}
}

func (s *indexSection) Select(ctx context.Context, prefix []byte) records.Select {
	section := make([]byte, 0, len(s.prefix)+len(prefix))
	section = append(section, s.prefix...)
	section = append(section, prefix...)
	return s.base.Select(ctx, section)
}

func (s *indexSection) Update(ctx context.Context) records.Update {
	panic("index section is read-only")
}

type indexUpdate struct {
	t *testing.T
	u records.Update
}

func openTx(t *testing.T, i records.Index) records.Update {
	return &indexUpdate{
		t: t,
		u: i.Update(context.Background()),
	}
}

func update(t *testing.T, i records.Index, f func(records.Update)) {
	tx := openTx(t, i)
	f(tx)
	tx.Commit()
	tx.Close()
}

func (i *indexUpdate) Close() error {
	return i.check("Close", i.u.Close())
}

func (i *indexUpdate) Insert(key []byte, offset int64) error {
	return i.check("Insert", i.u.Insert(key, offset))
}

func (i *indexUpdate) Delete(key []byte) error {
	return i.check("Delete", i.u.Delete(key))
}

func (i *indexUpdate) DropKeys(prefix []byte) error {
	return i.check("DropKeys", i.u.DropKeys(prefix))
}

func (i *indexUpdate) DropOffsets(limit int64) error {
	return i.check("DropOffsets", i.u.DropOffsets(limit))
}

func (i *indexUpdate) Commit() error {
	return i.check("Commit", i.u.Commit())
}

func (i *indexUpdate) check(name string, err error) error {
	i.t.Helper()
	i.t.Helper()
	if err != nil {
		i.t.Errorf("records.Index.%s: %v", name, err)
	}
	return err
}

type indexState []indexEntry

type indexEntry struct {
	key    string
	offset int64
}

func assertIndexState(t *testing.T, index records.Index, state indexState) {
	t.Helper()
	i := 0

	selection := index.Select(context.Background(), nil)
	defer selection.Close()

	for selection.Next() {
		if i >= len(state) {
			t.Errorf("too many entries exposed by index selection: %d/%d", i, len(state))
			return
		}
		key, offset := selection.Entry()
		if string(key) != state[i].key {
			t.Errorf("wrong key selected at index %d/%d: got=%q want=%q", i, len(state), key, state[i].key)
			return
		}
		if offset != state[i].offset {
			t.Errorf("wrong offset selected at index %d/%d: got=%d want=%d", i, len(state), offset, state[i].offset)
			return
		}
		i++
	}

	if err := selection.Close(); err != nil {
		t.Errorf("closing selection: %v", err)
	}

	if i < len(state) {
		t.Errorf("not enough entries exposed by the index selection: %d/%d", i, len(state))
	}
}
