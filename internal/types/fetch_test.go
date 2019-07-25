package types

func init() {
	topics0 := []FetchRequestTopic0{{
		Topic: "topic-0",
		Partitions: []FetchRequestPartition0{
			{
				Partition:         1,
				FetchOffset:       42,
				PartitionMaxBytes: 8192,
			},
			{
				Partition:         2,
				FetchOffset:       1234,
				PartitionMaxBytes: 8192,
			},
		},
	}}

	topics1 := []FetchRequestTopic1{{
		Topic: "topic-1",
		Partitions: []FetchRequestPartition1{
			{
				Partition:         1,
				FetchOffset:       42,
				LogStartOffset:    1234567890,
				PartitionMaxBytes: 8192,
			},
			{
				Partition:         2,
				FetchOffset:       1234,
				LogStartOffset:    987654321,
				PartitionMaxBytes: 8192,
			},
		},
	}}

	topics2 := []FetchRequestTopic2{{
		Topic: "topic-2",
		Partitions: []FetchRequestPartition2{
			{
				Partition:          1,
				CurrentLeaderEpoch: 567890,
				FetchOffset:        42,
				LogStartOffset:     1234567890,
				PartitionMaxBytes:  8192,
			},
			{
				Partition:          2,
				CurrentLeaderEpoch: 98765,
				FetchOffset:        1234,
				LogStartOffset:     987654321,
				PartitionMaxBytes:  8192,
			},
		},
	}}

	forgottenTopics := []FetchRequestForgottenTopics{{
		Topic:      "topic-A",
		Partitions: []int32{0, 1, 2, 3, 4},
	}}

	responses0 := []FetchResponseTopic0{{
		Topic: "topic-0",
		PartitionResponses: []FetchResponsePartition0{{
			Partition:     1,
			ErrorCode:     2,
			HighWatermark: 3,
			RecordSet: [][]byte{
				[]byte(`!`),
				[]byte(`0987654321`),
				[]byte(`1234567890`),
			},
		}},
	}}

	responses1 := []FetchResponseTopic1{{
		Topic: "topic-0",
		PartitionResponses: []FetchResponsePartition1{{
			Partition:        1,
			ErrorCode:        2,
			HighWatermark:    3,
			LastStableOffset: 4,
			AbortedTransactions: []FetchResponseAbortedTransactions{{
				ProducerID:  5,
				FirstOffset: 6,
			}},
			RecordSet: [][]byte{
				[]byte(`!`),
				[]byte(`0987654321`),
				[]byte(`1234567890`),
			},
		}},
	}}

	addTestValues(
		&FetchRequestV0{
			ReplicaID:   1,
			MaxWaitTime: 1000,
			MinBytes:    1024,
			Topics:      topics0,
		},

		&FetchRequestV1{
			ReplicaID:   1,
			MaxWaitTime: 1000,
			MinBytes:    1024,
			Topics:      topics0,
		},

		&FetchRequestV2{
			ReplicaID:   1,
			MaxWaitTime: 1000,
			MinBytes:    1024,
			Topics:      topics0,
		},

		&FetchRequestV3{
			ReplicaID:   1,
			MaxWaitTime: 1000,
			MinBytes:    1024,
			MaxBytes:    8192,
			Topics:      topics0,
		},

		&FetchRequestV4{
			ReplicaID:      1,
			MaxWaitTime:    1000,
			MinBytes:       1024,
			MaxBytes:       8192,
			IsolationLevel: 2,
			Topics:         topics0,
		},

		&FetchRequestV5{
			ReplicaID:      1,
			MaxWaitTime:    1000,
			MinBytes:       1024,
			MaxBytes:       8192,
			IsolationLevel: 2,
			Topics:         topics1,
		},

		&FetchRequestV6{
			ReplicaID:      1,
			MaxWaitTime:    1000,
			MinBytes:       1024,
			MaxBytes:       8192,
			IsolationLevel: 2,
			Topics:         topics1,
		},

		&FetchRequestV7{
			ReplicaID:           1,
			MaxWaitTime:         1000,
			MinBytes:            1024,
			MaxBytes:            8192,
			IsolationLevel:      2,
			SessionID:           3,
			SessionEpoch:        4,
			Topics:              topics1,
			ForgottenTopicsData: forgottenTopics,
		},

		&FetchRequestV8{
			ReplicaID:           1,
			MaxWaitTime:         1000,
			MinBytes:            1024,
			MaxBytes:            8192,
			IsolationLevel:      2,
			SessionID:           3,
			SessionEpoch:        4,
			Topics:              topics1,
			ForgottenTopicsData: forgottenTopics,
		},

		&FetchRequestV9{
			ReplicaID:           1,
			MaxWaitTime:         1000,
			MinBytes:            1024,
			MaxBytes:            8192,
			IsolationLevel:      2,
			SessionID:           3,
			SessionEpoch:        4,
			Topics:              topics2,
			ForgottenTopicsData: forgottenTopics,
		},

		&FetchRequestV10{
			ReplicaID:           1,
			MaxWaitTime:         1000,
			MinBytes:            1024,
			MaxBytes:            8192,
			IsolationLevel:      2,
			SessionID:           3,
			SessionEpoch:        4,
			Topics:              topics2,
			ForgottenTopicsData: forgottenTopics,
		},

		&FetchResponseV0{
			Responses: responses0,
		},

		&FetchResponseV1{
			ThrottleTimeMs: 123,
			Responses:      responses0,
		},

		&FetchResponseV2{
			ThrottleTimeMs: 123,
			Responses:      responses0,
		},

		&FetchResponseV3{
			ThrottleTimeMs: 123,
			Responses:      responses0,
		},

		&FetchResponseV4{
			ThrottleTimeMs: 123,
			Responses:      responses1,
		},
	)
}
