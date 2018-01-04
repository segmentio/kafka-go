package kafka

import (
	"hash"
	"hash/crc32"
	"testing"
)

func TestHashBalancer(t *testing.T) {
	testCases := map[string]struct {
		Key        []byte
		Hasher     hash.Hash32
		Partitions []int
		Partition  int
	}{
		"nil": {
			Key:        nil,
			Partitions: []int{0, 1, 2},
			Partition:  0,
		},
		"partition-0": {
			Key:        []byte("blah"),
			Partitions: []int{0, 1},
			Partition:  0,
		},
		"partition-1": {
			Key:        []byte("blah"),
			Partitions: []int{0, 1, 2},
			Partition:  1,
		},
		"partition-2": {
			Key:        []byte("boop"),
			Partitions: []int{0, 1, 2},
			Partition:  2,
		},
		"custom hash": {
			Key:        []byte("boop"),
			Hasher:     crc32.NewIEEE(),
			Partitions: []int{0, 1, 2},
			Partition:  1,
		},
	}

	for label, test := range testCases {
		t.Run(label, func(t *testing.T) {
			msg := Message{Key: test.Key}
			h := Hash{
				Hasher: test.Hasher,
			}
			partition := h.Balance(msg, test.Partitions...)
			if partition != test.Partition {
				t.Errorf("expected %v; got %v", test.Partition, partition)
			}
		})
	}
}
