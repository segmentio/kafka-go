package protocol

type ListOffsetsRequest struct {
	ReplicaID      int32                     `kafka:"min=v1,max=v5"`
	IsolationLevel int8                      `kafka:"min=v2,max=v5"`
	Topics         []ListOffsetsRequestTopic `kafka:"min=v1,max=v5"`
}

func (r *ListOffsetsRequest) ApiKey() ApiKey { return ListOffsets }

type ListOffsetsRequestTopic struct {
	Topic      string                        `kafka:"min=v1,max=v5"`
	Partitions []ListOffsetsRequestPartition `kafka:"min=v1,max=v5"`
}

type ListOffsetsRequestPartition struct {
	Partition          int32 `kafka:"min=v1,max=v5"`
	CurrentLeaderEpoch int32 `kafka:"min=v4,max=v5"`
	Timestamp          int64 `kafka:"min=v1,max=v5"`
}

type ListOffsetsResponse struct {
	Topics []ListOffsetsResponseTopic `kafka:"min=v1,max=v5"`
}

func (r *ListOffsetsResponse) ApiKey() ApiKey { return ListOffsets }

type ListOffsetsResponseTopic struct {
	Topic      string                         `kafka:"min=v1,max=v5"`
	Partitions []ListOffsetsResponsePartition `kafka:"min=v1,max=v5"`
}

type ListOffsetsResponsePartition struct {
	ThrottleTimeMs int32 `kafka:"min=v2,max=v5"`
	Partition      int32 `kafka:"min=v1,max=v5"`
	ErrorCode      int16 `kafka:"min=v1,max=v5"`
	Timestamp      int64 `kafka:"min=v1,max=v5"`
	Offset         int64 `kafka:"min=v1,max=v5"`
	LeaderEpoch    int32 `kafka:"min=v4,max=v5"`
}
