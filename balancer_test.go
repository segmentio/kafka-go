package kafka

import (
	"fmt"
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
		// in a previous version, this test would select a different partition
		// than sarama's hash partitioner.
		"hash code with MSB set": {
			Key:        []byte("20"),
			Partitions: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
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

func TestReferenceHashBalancer(t *testing.T) {
	testCases := map[string]struct {
		Key               []byte
		Hasher            hash.Hash32
		Partitions        []int
		Partition         int
		RndBalancerResult int
	}{
		"nil": {
			Key:               nil, // nil key means random partition
			Partitions:        []int{0, 1, 2},
			Partition:         123,
			RndBalancerResult: 123,
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
			Key:        []byte("castle"),
			Partitions: []int{0, 1, 2},
			Partition:  2,
		},
		"custom hash": {
			Key:        []byte("boop"),
			Hasher:     crc32.NewIEEE(),
			Partitions: []int{0, 1, 2},
			Partition:  1,
		},
		"hash code with MSB set": {
			Key:        []byte("20"),
			Partitions: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Partition:  15,
		},
	}

	for label, test := range testCases {
		t.Run(label, func(t *testing.T) {
			var rr randomBalancer
			if test.Key == nil {
				rr.mock = test.RndBalancerResult
			}

			msg := Message{Key: test.Key}
			h := ReferenceHash{Hasher: test.Hasher, rr: rr}
			partition := h.Balance(msg, test.Partitions...)
			if partition != test.Partition {
				t.Errorf("expected %v; got %v", test.Partition, partition)
			}
		})
	}
}

func TestCRC32Balancer(t *testing.T) {
	// These tests are taken from the default "consistent_random" partitioner from
	// https://github.com/edenhill/librdkafka/blob/master/tests/0048-partitioner.c
	partitionCount := 17
	var partitions []int
	for i := 0; i < partitionCount; i++ {
		partitions = append(partitions, i*i)
	}

	testCases := map[string]struct {
		Key        []byte
		Partitions []int
		Partition  int
	}{
		"nil": {
			Key:        nil,
			Partitions: partitions,
			Partition:  -1,
		},
		"empty": {
			Key:        []byte{},
			Partitions: partitions,
			Partition:  -1,
		},
		"unaligned": {
			Key:        []byte("23456"),
			Partitions: partitions,
			Partition:  partitions[0xb1b451d7%partitionCount],
		},
		"long key": {
			Key:        []byte("this is another string with more length to it perhaps"),
			Partitions: partitions,
			Partition:  partitions[0xb0150df7%partitionCount],
		},
		"short key": {
			Key:        []byte("hejsan"),
			Partitions: partitions,
			Partition:  partitions[0xd077037e%partitionCount],
		},
	}

	t.Run("default", func(t *testing.T) {
		for label, test := range testCases {
			t.Run(label, func(t *testing.T) {
				b := CRC32Balancer{}
				b.random.mock = -1

				msg := Message{Key: test.Key}
				partition := b.Balance(msg, test.Partitions...)
				if partition != test.Partition {
					t.Errorf("expected %v; got %v", test.Partition, partition)
				}
			})
		}
	})

	t.Run("consistent", func(t *testing.T) {
		b := CRC32Balancer{Consistent: true}
		b.random.mock = -1

		p := b.Balance(Message{}, partitions...)
		if p < 0 {
			t.Fatal("should not have gotten a random partition")
		}
		for i := 0; i < 10; i++ {
			if p != b.Balance(Message{}, partitions...) {
				t.Fatal("nil key should always hash consistently")
			}
			if p != b.Balance(Message{Key: []byte{}}, partitions...) {
				t.Fatal("empty key should always hash consistently and have same result as nil key")
			}
		}
	})
}

func TestMurmur2(t *testing.T) {
	// These tests are taken from the "murmur2" implementation from
	// https://github.com/edenhill/librdkafka/blob/master/src/rdmurmur2.c
	testCases := []struct {
		Key               []byte
		JavaMurmur2Result uint32
	}{
		{Key: []byte("kafka"), JavaMurmur2Result: 0xd067cf64},
		{Key: []byte("giberish123456789"), JavaMurmur2Result: 0x8f552b0c},
		{Key: []byte("1234"), JavaMurmur2Result: 0x9fc97b14},
		{Key: []byte("234"), JavaMurmur2Result: 0xe7c009ca},
		{Key: []byte("34"), JavaMurmur2Result: 0x873930da},
		{Key: []byte("4"), JavaMurmur2Result: 0x5a4b5ca1},
		{Key: []byte("PreAmbleWillBeRemoved,ThePrePartThatIs"), JavaMurmur2Result: 0x78424f1c},
		{Key: []byte("reAmbleWillBeRemoved,ThePrePartThatIs"), JavaMurmur2Result: 0x4a62b377},
		{Key: []byte("eAmbleWillBeRemoved,ThePrePartThatIs"), JavaMurmur2Result: 0xe0e4e09e},
		{Key: []byte("AmbleWillBeRemoved,ThePrePartThatIs"), JavaMurmur2Result: 0x62b8b43f},
		{Key: []byte(""), JavaMurmur2Result: 0x106e08d9},
		{Key: nil, JavaMurmur2Result: 0x106e08d9},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("key:%s", test.Key), func(t *testing.T) {
			got := murmur2(test.Key)
			if got != test.JavaMurmur2Result {
				t.Errorf("expected %v; got %v", test.JavaMurmur2Result, got)
			}
		})
	}
}

func TestMurmur2Balancer(t *testing.T) {
	// These tests are taken from the "murmur2_random" partitioner from
	// https://github.com/edenhill/librdkafka/blob/master/tests/0048-partitioner.c
	partitionCount := 17
	librdkafkaPartitions := make([]int, partitionCount)
	for i := 0; i < partitionCount; i++ {
		librdkafkaPartitions[i] = i * i
	}

	// These tests are taken from the Murmur2Partitioner Python class from
	// https://github.com/dpkp/kafka-python/blob/master/test/test_partitioner.py
	pythonPartitions := make([]int, 1000)
	for i := 0; i < 1000; i++ {
		pythonPartitions[i] = i
	}

	testCases := map[string]struct {
		Key        []byte
		Partitions []int
		Partition  int
	}{
		"librdkafka-nil": {
			Key:        nil,
			Partitions: librdkafkaPartitions,
			Partition:  123,
		},
		"librdkafka-empty": {
			Key:        []byte{},
			Partitions: librdkafkaPartitions,
			Partition:  librdkafkaPartitions[0x106e08d9%partitionCount],
		},
		"librdkafka-unaligned": {
			Key:        []byte("23456"),
			Partitions: librdkafkaPartitions,
			Partition:  librdkafkaPartitions[0x058d780f%partitionCount],
		},
		"librdkafka-long key": {
			Key:        []byte("this is another string with more length to it perhaps"),
			Partitions: librdkafkaPartitions,
			Partition:  librdkafkaPartitions[0x4f7703da%partitionCount],
		},
		"librdkafka-short key": {
			Key:        []byte("hejsan"),
			Partitions: librdkafkaPartitions,
			Partition:  librdkafkaPartitions[0x5ec19395%partitionCount],
		},
		"python-empty": {
			Key:        []byte(""),
			Partitions: pythonPartitions,
			Partition:  681,
		},
		"python-a": {
			Key:        []byte("a"),
			Partitions: pythonPartitions,
			Partition:  524,
		},
		"python-ab": {
			Key:        []byte("ab"),
			Partitions: pythonPartitions,
			Partition:  434,
		},
		"python-abc": {
			Key:        []byte("abc"),
			Partitions: pythonPartitions,
			Partition:  107,
		},
		"python-123456789": {
			Key:        []byte("123456789"),
			Partitions: pythonPartitions,
			Partition:  566,
		},
		"python-\x00 ": {
			Key:        []byte{0, 32},
			Partitions: pythonPartitions,
			Partition:  742,
		},
	}

	t.Run("default", func(t *testing.T) {
		for label, test := range testCases {
			t.Run(label, func(t *testing.T) {
				b := Murmur2Balancer{}
				b.random.mock = 123

				msg := Message{Key: test.Key}
				partition := b.Balance(msg, test.Partitions...)
				if partition != test.Partition {
					t.Errorf("expected %v; got %v", test.Partition, partition)
				}
			})
		}
	})

	t.Run("consistent", func(t *testing.T) {
		b := Murmur2Balancer{Consistent: true}
		b.random.mock = -1

		p := b.Balance(Message{}, librdkafkaPartitions...)
		if p < 0 {
			t.Fatal("should not have gotten a random partition")
		}
		for i := 0; i < 10; i++ {
			if p != b.Balance(Message{}, librdkafkaPartitions...) {
				t.Fatal("nil key should always hash consistently")
			}
		}
	})
}

func TestLeastBytes(t *testing.T) {
	testCases := map[string]struct {
		Keys       [][]byte
		Partitions [][]int
		Partition  int
	}{
		"single message": {
			Keys: [][]byte{
				[]byte("key"),
			},
			Partitions: [][]int{
				{0, 1, 2},
			},
			Partition: 0,
		},
		"multiple messages, no partition change": {
			Keys: [][]byte{
				[]byte("a"),
				[]byte("ab"),
				[]byte("abc"),
				[]byte("abcd"),
			},
			Partitions: [][]int{
				{0, 1, 2},
				{0, 1, 2},
				{0, 1, 2},
				{0, 1, 2},
			},
			Partition: 0,
		},
		"partition gained": {
			Keys: [][]byte{
				[]byte("hello world 1"),
				[]byte("hello world 2"),
				[]byte("hello world 3"),
			},
			Partitions: [][]int{
				{0, 1},
				{0, 1},
				{0, 1, 2},
			},
			Partition: 0,
		},
		"partition lost": {
			Keys: [][]byte{
				[]byte("hello world 1"),
				[]byte("hello world 2"),
				[]byte("hello world 3"),
			},
			Partitions: [][]int{
				{0, 1, 2},
				{0, 1, 2},
				{0, 1},
			},
			Partition: 0,
		},
	}

	for label, test := range testCases {
		t.Run(label, func(t *testing.T) {
			lb := &LeastBytes{}

			var partition int
			for i, key := range test.Keys {
				msg := Message{Key: key}
				partition = lb.Balance(msg, test.Partitions[i]...)
			}

			if partition != test.Partition {
				t.Errorf("expected %v; got %v", test.Partition, partition)
			}
		})
	}
}

func TestRoundRobin(t *testing.T) {
	testCases := map[string]struct {
		Partitions []int
		ChunkSize  int
	}{
		"default - odd partition count": {
			Partitions: []int{0, 1, 2, 3, 4, 5, 6},
		},
		"negative chunk size - odd partition count": {
			Partitions: []int{0, 1, 2, 3, 4, 5, 6},
			ChunkSize:  -1,
		},
		"0 chunk size - odd partition count": {
			Partitions: []int{0, 1, 2, 3, 4, 5, 6},
			ChunkSize:  0,
		},
		"5 chunk size - odd partition count": {
			Partitions: []int{0, 1, 2, 3, 4, 5, 6},
			ChunkSize:  5,
		},
		"12 chunk size - odd partition count": {
			Partitions: []int{0, 1, 2, 3, 4, 5, 6},
			ChunkSize:  12,
		},
		"default - even partition count": {
			Partitions: []int{0, 1, 2, 3, 4, 5, 6, 7},
		},
		"negative chunk size - even partition count": {
			Partitions: []int{0, 1, 2, 3, 4, 5, 6, 7},
			ChunkSize:  -1,
		},
		"0 chunk size - even partition count": {
			Partitions: []int{0, 1, 2, 3, 4, 5, 6, 7},
			ChunkSize:  0,
		},
		"5 chunk size - even partition count": {
			Partitions: []int{0, 1, 2, 3, 4, 5, 6, 7},
			ChunkSize:  5,
		},
		"12 chunk size - even partition count": {
			Partitions: []int{0, 1, 2, 3, 4, 5, 6, 7},
			ChunkSize:  12,
		},
	}
	for label, test := range testCases {
		t.Run(label, func(t *testing.T) {
			lb := &RoundRobin{ChunkSize: test.ChunkSize}
			msg := Message{}
			var partition int
			var i int
			expectedChunkSize := test.ChunkSize
			if expectedChunkSize < 1 {
				expectedChunkSize = 1
			}
			partitions := test.Partitions
			for i = 0; i < 50; i++ {
				partition = lb.Balance(msg, partitions...)
				if partition != i/expectedChunkSize%len(partitions) {
					t.Error("Returned partition", partition, "expecting", i/expectedChunkSize%len(partitions))
				}
			}
		})
	}
}
