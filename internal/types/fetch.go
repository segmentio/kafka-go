package types

type FetchRequestV0 struct {
	ReplicaID   int32
	MaxWaitTime int32
	MinBytes    int32
	Topics      []FetchRequestTopic0
}

type FetchRequestV1 FetchRequestV0

type FetchRequestV2 FetchRequestV1

type FetchRequestV3 struct {
	ReplicaID   int32
	MaxWaitTime int32
	MinBytes    int32
	MaxBytes    int32
	Topics      []FetchRequestTopic0
}

type FetchRequestV4 struct {
	ReplicaID      int32
	MaxWaitTime    int32
	MinBytes       int32
	MaxBytes       int32
	IsolationLevel int8
	Topics         []FetchRequestTopic0
}

type FetchRequestV5 struct {
	ReplicaID      int32
	MaxWaitTime    int32
	MinBytes       int32
	MaxBytes       int32
	IsolationLevel int8
	Topics         []FetchRequestTopic1
}

type FetchRequestV6 FetchRequestV5

type FetchRequestV7 struct {
	ReplicaID           int32
	MaxWaitTime         int32
	MinBytes            int32
	MaxBytes            int32
	IsolationLevel      int8
	SessionID           int32
	SessionEpoch        int32
	Topics              []FetchRequestTopic1
	ForgottenTopicsData []FetchRequestForgottenTopics
}

type FetchRequestV8 FetchRequestV7

type FetchRequestV9 struct {
	ReplicaID           int32
	MaxWaitTime         int32
	MinBytes            int32
	MaxBytes            int32
	IsolationLevel      int8
	SessionID           int32
	SessionEpoch        int32
	Topics              []FetchRequestTopic2
	ForgottenTopicsData []FetchRequestForgottenTopics
}

type FetchRequestV10 FetchRequestV9

type FetchRequestTopic0 struct {
	Topic      string
	Partitions []FetchRequestPartition0
}

type FetchRequestPartition0 struct {
	Partition         int32
	FetchOffset       int64
	PartitionMaxBytes int32
}

type FetchRequestTopic1 struct {
	Topic      string
	Partitions []FetchRequestPartition1
}

type FetchRequestPartition1 struct {
	Partition         int32
	FetchOffset       int64
	LogStartOffset    int64
	PartitionMaxBytes int32
}

type FetchRequestTopic2 struct {
	Topic      string
	Partitions []FetchRequestPartition2
}

type FetchRequestPartition2 struct {
	Partition          int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LogStartOffset     int64
	PartitionMaxBytes  int32
}

type FetchRequestForgottenTopics struct {
	Topic      string
	Partitions []int32
}

type FetchResponseV0 struct {
	Responses []FetchResponseTopic0
}

type FetchResponseV1 struct {
	ThrottleTimeMs int32
	Responses      []FetchResponseTopic0
}

type FetchResponseV2 FetchResponseV1

type FetchResponseV3 FetchResponseV2

type FetchResponseV4 struct {
	ThrottleTimeMs int32
	Responses      []FetchResponseTopic1
}

type FetchResponseTopic0 struct {
	Topic              string
	PartitionResponses []FetchResponsePartition0
}

type FetchResponsePartition0 struct {
	Partition     int32
	ErrorCode     int16
	HighWatermark int64
	RecordSet     [][]byte
}

type FetchResponseTopic1 struct {
	Topic              string
	PartitionResponses []FetchResponsePartition1
}

type FetchResponsePartition1 struct {
	Partition           int32
	ErrorCode           int16
	HighWatermark       int64
	LastStableOffset    int64
	AbortedTransactions []FetchResponseAbortedTransactions
	RecordSet           [][]byte
}

type FetchResponseTopic2 struct {
	Topic              string
	PartitionResponses []FetchResponsePartition2
}

type FetchResponsePartition2 struct {
	Partition           int32
	ErrorCode           int16
	HighWatermark       int64
	LastStableOffset    int64
	LogStartOffset      int64
	AbortedTransactions []FetchResponseAbortedTransactions
	RecordSet           [][]byte
}

type FetchResponseAbortedTransactions struct {
	ProducerID  int64
	FirstOffset int64
}
