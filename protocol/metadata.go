package protocol

type MetadataRequest struct {
	TopicNames                          []string `kafka:"min=v0,max=v8,nullable|min=v9,max=v9,compact,nullable"`
	AllowAutoTopicCreation              bool     `kafka:"min=v4,max=v9"`
	IncludeCustomerAuthorizedOperations bool     `kafka:"min=v8,max=v9"`
	IncludeTopicAuthorizedOperations    bool     `kafka:"min=v8,max=v9"`
}

func (r *MetadataRequest) ApiKey() ApiKey { return Metadata }

type MetadataResponse struct {
	ThrottleTimeMs              int32                    `kafka:"min=v3,max=v9"`
	Brokers                     []MetadataResponseBroker `kafka:"min=v0,max=v9"`
	ClientID                    string                   `kafka:"min=v2,max=v9,nullable"`
	ControllerID                int32                    `kafka:"min=v1,max=v9"`
	Topics                      []MetadataResponseTopic  `kafka:"min=v0,max=v9"`
	ClusterAuthorizedOperations int32                    `kafka:"min=v8,max=v9"`
}

func (r *MetadataResponse) ApiKey() ApiKey { return Metadata }

type MetadataResponseBroker struct {
	NodeID int32  `kafka:"min=v0,max=v9"`
	Host   string `kafka:"min=v0,max=v8|min=v9,max=v9,compact"`
	Port   int32  `kafka:"min=v0,max=v9"`
	Rack   string `kafka:"min=v1,max=v9,nullable|min=v9,max=v9,compact,nullable"`
}

type MetadataResponseTopic struct {
	ErrorCode                 int16                       `kafka:"min=v0,max=v9"`
	Name                      string                      `kafka:"min=v0,max=v9,nullable|min=v9,max=v9,compact,nullable"`
	IsInternal                bool                        `kafka:"min=v1,max=v9"`
	Partitions                []MetadataResponsePartition `kafka:"min=v0,max=v9"`
	TopicAuthorizedOperations int32                       `kafka:"min=v8,max=v9"`
}

type MetadataResponsePartition struct {
	ErrorCode       int16   `kafka:"min=v0,max=v9"`
	PartitionIndex  int32   `kafka:"min=v0,max=v9"`
	LeaderID        int32   `kafka:"min=v0,max=v9"`
	LeaderEpoch     int32   `kafka:"min=v7,max=v9"`
	ReplicaNodes    []int32 `kafka:"min=v0,max=v9"`
	IsrNodes        []int32 `kafka:"min=v0,max=v9"`
	OfflineReplicas []int32 `kafka:"min=v5,max=v9"`
}
