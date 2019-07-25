package types

type ProduceRequestV0 struct {
	Acks      int16
	Timeout   int32
	TopicData []ProduceRequestTopic0
}

type ProduceRequestV1 ProduceRequestV0

type ProduceRequestV2 ProduceRequestV1

type ProduceRequestV3 struct {
	TransactionalID NullableString
	Acks            int16
	Timeout         int32
	TopicData       []ProduceRequestTopic0
}

type ProduceRequestV4 ProduceRequestV3

type ProduceRequestV5 ProduceRequestV4

type ProduceRequestV6 ProduceRequestV5

type ProduceRequestV7 ProduceRequestV6

type ProduceRequestTopic0 struct {
	Topic string
	Data  []ProduceRequestPartition0
}

type ProduceRequestPartition0 struct {
	Partition int32
	RecordSet [][]byte // TODO: interface{} ?
}

type ProduceResponseV0 struct {
	Responses []ProduceResponseTopic0
}

type ProduceResponseV1 struct {
	Responses      []ProduceResponseTopic0
	ThrottleTimeMs int32
}

type ProduceResponseV2 struct {
	Responses      []ProduceResponseTopic1
	ThrottleTimeMs int32
}

type ProduceResponseV3 ProduceResponseV2

type ProduceResponseV4 ProduceResponseV3

type ProduceResponseV5 struct {
	Responses      []ProduceResponseTopic2
	ThrottleTimeMs int32
}

type ProduceResponseV6 ProduceResponseV5

type ProduceResponseV7 ProduceResponseV6

type ProduceResponseTopic0 struct {
	Topic              string
	PartitionResponses []ProduceResponsePartition0
}

type ProduceResponsePartition0 struct {
	Partition  int32
	ErrorCode  int16
	BaseOffset int64
}

type ProduceResponseTopic1 struct {
	Topic              string
	PartitionResponses []ProduceResponsePartition1
}

type ProduceResponsePartition1 struct {
	Partition     int32
	ErrorCode     int16
	BaseOffset    int64
	LogAppendTime int64
}

type ProduceResponseTopic2 struct {
	Topic              string
	PartitionResponses []ProduceResponsePartition2
}

type ProduceResponsePartition2 struct {
	Partition      int32
	ErrorCode      int16
	BaseOffset     int64
	LogAppendTime  int64
	LogStartOffset int64
}
