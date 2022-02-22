package records_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/records"
)

type storageOperation interface {
	apply(context.Context, records.Storage) error
}

type insertOperation struct {
	key   records.Key
	value []byte
}

func (op insertOperation) apply(ctx context.Context, store records.Storage) error {
	_, err := store.Store(ctx, op.key, bytes.NewReader(op.value))
	return err
}

type deleteOperation struct {
	keys []records.Key
}

func (op deleteOperation) apply(ctx context.Context, store records.Storage) error {
	return store.Delete(ctx, op.keys)
}

func TestRecordsStorage(t *testing.T) {
	tests := []struct {
		scenario string
		function func() (records.Storage, func(), error)
	}{
		{
			scenario: "memory",
			function: func() (records.Storage, func(), error) {
				return records.NewStorage(), func() {}, nil
			},
		},

		{
			scenario: "file-system",
			function: func() (records.Storage, func(), error) {
				p, err := os.MkdirTemp("", "kafka-go_records_*")
				if err != nil {
					return nil, nil, err
				}
				f, err := records.Mount(p)
				if err != nil {
					os.Remove(p)
					return nil, nil, err
				}
				return f, func() { os.Remove(p) }, nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			testRecordStorage(t, test.function)
		})
	}
}

func testRecordStorage(t *testing.T, makeStorage func() (records.Storage, func(), error)) {
	makeKey := func(addr, topic string, partition int, baseOffset int64) records.Key {
		return records.Key{
			Addr:       addr,
			Topic:      topic,
			Partition:  partition,
			BaseOffset: baseOffset,
		}
	}

	tests := []struct {
		scenario   string
		operations []storageOperation
		expected   map[records.Key][]byte
	}{
		{
			scenario: "an empty store does not contain any keys",
		},

		{
			scenario: "values inserted in a store are found when loading the associated keys",
			operations: []storageOperation{
				insertOperation{
					key:   makeKey("localhost:9092", "topic-0", 0, 0),
					value: []byte("hello"),
				},
				insertOperation{
					key:   makeKey("localhost:9092", "topic-0", 0, 1),
					value: []byte("world"),
				},
				insertOperation{
					key:   makeKey("localhost:9092", "topic-1", 1, 42),
					value: []byte("answer"),
				},
			},
			expected: map[records.Key][]byte{
				makeKey("localhost:9092", "topic-0", 0, 0):  []byte("hello"),
				makeKey("localhost:9092", "topic-0", 0, 1):  []byte("world"),
				makeKey("localhost:9092", "topic-1", 1, 42): []byte("answer"),
			},
		},

		{
			scenario: "keys deleted from the store are not visible anymore when loaded",
			operations: []storageOperation{
				insertOperation{
					key:   makeKey("localhost:9092", "topic-0", 0, 0),
					value: []byte("hello"),
				},
				insertOperation{
					key:   makeKey("localhost:9092", "topic-0", 0, 1),
					value: []byte("world"),
				},
				insertOperation{
					key:   makeKey("localhost:9092", "topic-1", 1, 42),
					value: []byte("answer"),
				},
				deleteOperation{
					keys: []records.Key{
						makeKey("localhost:9092", "topic-0", 0, 0),
						makeKey("localhost:9092", "topic-0", 0, 1),
					},
				},
			},
			expected: map[records.Key][]byte{
				makeKey("localhost:9092", "topic-1", 1, 42): []byte("answer"),
			},
		},

		{
			scenario: "keys inserted multiple times are deduplicated an the last value is retained",
			operations: []storageOperation{
				insertOperation{
					key:   makeKey("localhost:9092", "topic-0", 0, 1),
					value: []byte("hello"),
				},
				insertOperation{
					key:   makeKey("localhost:9092", "topic-0", 0, 1),
					value: []byte("world"),
				},
			},
			expected: map[records.Key][]byte{
				makeKey("localhost:9092", "topic-0", 0, 1): []byte("world"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			storage, teardown, err := makeStorage()
			if err != nil {
				t.Fatal("creating storage:", err)
			}
			defer teardown()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			for i, op := range test.operations {
				if err := op.apply(ctx, storage); err != nil {
					t.Fatalf("applying storage operation %d/%d: %v", i+1, len(test.operations), err)
				}
			}

			if _, err := storage.Load(ctx, makeKey("whatever", "any", 0, 0)); !errors.Is(err, records.ErrNotFound) {
				t.Errorf("looking up a key which does not exist in the store did not return records.ErrNotFound: %v", err)
			}

			state := make(map[records.Key][]byte, len(test.expected))
			for key, value := range test.expected {
				state[key] = value
			}

			it := storage.List(ctx)
			defer it.Close()

			buf := new(bytes.Buffer)
			for it.Next() {
				key := it.Key()

				r, err := storage.Load(ctx, key)
				if err != nil {
					t.Errorf("loading storage key %+v: %v", key, err)
					continue
				}

				want := state[key]
				delete(state, key)

				buf.Reset()
				buf.Grow(int(it.Size()))
				if _, err := buf.ReadFrom(r); err != nil {
					r.Close()
					t.Errorf("reading storage key %+v: %v", key, err)
					continue
				}

				if err := r.Close(); err != nil {
					t.Errorf("closing storage value of %+v: %v", key, err)
				}

				if got := buf.Bytes(); !bytes.Equal(want, got) {
					t.Errorf("value of storage key %+v mismatch: want=%q got=%q", key, want, got)
				}

				if got := it.Size(); int64(len(want)) != got {
					t.Errorf("value length of storage key %+v mismatch: want=%d got=%d", key, len(want), got)
				}
			}

			if err := it.Close(); err != nil {
				t.Error("closing storage iterator:", err)
			}

			if len(state) > 0 {
				t.Errorf("%d keys were not found in the store:", len(state))
				for key, value := range state {
					t.Logf("\t%+v => %q", key, value)
				}
			}
		})
	}
}
