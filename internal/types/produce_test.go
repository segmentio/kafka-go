package types

func init() {
	topicData := []ProduceRequestTopic0{
		{
			Topic: "A",
			Data: []ProduceRequestPartition0{
				{
					Partition: 1,
					RecordSet: [][]byte{
						[]byte(`1234567890`),
						[]byte(`0987654321`),
						[]byte(`!`),
					},
				},
				{
					Partition: 2,
					RecordSet: [][]byte{
						[]byte(`!`),
						[]byte(`0987654321`),
						[]byte(`1234567890`),
					},
				},
			},
		},

		{
			Topic: "B",
		},
	}

	topicResponses0 := []ProduceResponseTopic0{
		{
			Topic: "A",
			PartitionResponses: []ProduceResponsePartition0{
				{
					Partition:  0,
					ErrorCode:  0,
					BaseOffset: 0,
				},
				{
					Partition:  1,
					ErrorCode:  2,
					BaseOffset: 3,
				},
			},
		},
		{
			Topic: "B",
		},
	}

	topicResponses1 := []ProduceResponseTopic1{
		{
			Topic: "A",
			PartitionResponses: []ProduceResponsePartition1{
				{
					Partition:     0,
					ErrorCode:     0,
					BaseOffset:    0,
					LogAppendTime: 1234567890,
				},
				{
					Partition:     1,
					ErrorCode:     2,
					BaseOffset:    3,
					LogAppendTime: 1234567890,
				},
			},
		},
		{
			Topic: "B",
		},
	}

	topicResponses2 := []ProduceResponseTopic2{
		{
			Topic: "A",
			PartitionResponses: []ProduceResponsePartition2{
				{
					Partition:      0,
					ErrorCode:      0,
					BaseOffset:     0,
					LogAppendTime:  1234567890,
					LogStartOffset: 42,
				},
				{
					Partition:      1,
					ErrorCode:      2,
					BaseOffset:     3,
					LogAppendTime:  1234567890,
					LogStartOffset: 42,
				},
			},
		},
		{
			Topic: "B",
		},
	}

	addTestValues(
		&ProduceRequestV0{
			Acks:      1,
			Timeout:   1234,
			TopicData: topicData,
		},

		&ProduceRequestV1{
			Acks:      1,
			Timeout:   1234,
			TopicData: topicData,
		},

		&ProduceRequestV2{
			Acks:      1,
			Timeout:   1234,
			TopicData: topicData,
		},

		&ProduceRequestV3{
			TransactionalID: "1234567890",
			Acks:            1,
			Timeout:         1234,
			TopicData:       topicData,
		},

		&ProduceRequestV4{
			TransactionalID: "1234567890",
			Acks:            1,
			Timeout:         1234,
			TopicData:       topicData,
		},

		&ProduceRequestV5{
			TransactionalID: "1234567890",
			Acks:            1,
			Timeout:         1234,
			TopicData:       topicData,
		},

		&ProduceRequestV6{
			TransactionalID: "1234567890",
			Acks:            1,
			Timeout:         1234,
			TopicData:       topicData,
		},

		&ProduceRequestV7{
			TransactionalID: "1234567890",
			Acks:            1,
			Timeout:         1234,
			TopicData:       topicData,
		},

		&ProduceResponseV0{
			Responses: topicResponses0,
		},

		&ProduceResponseV1{
			Responses:      topicResponses0,
			ThrottleTimeMs: 123,
		},

		&ProduceResponseV2{
			Responses:      topicResponses1,
			ThrottleTimeMs: 123,
		},

		&ProduceResponseV3{
			Responses:      topicResponses1,
			ThrottleTimeMs: 123,
		},

		&ProduceResponseV4{
			Responses:      topicResponses1,
			ThrottleTimeMs: 123,
		},

		&ProduceResponseV5{
			Responses:      topicResponses2,
			ThrottleTimeMs: 123,
		},

		&ProduceResponseV6{
			Responses:      topicResponses2,
			ThrottleTimeMs: 123,
		},

		&ProduceResponseV7{
			Responses:      topicResponses2,
			ThrottleTimeMs: 123,
		},
	)
}
