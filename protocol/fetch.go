package protocol

type FetchRequest struct {
	ReplicaID       int32                        `kafka:"min=v0,max=v11"`
	MaxWaitTime     int32                        `kafka:"min=v0,max=v11"`
	MinBytes        int32                        `kafka:"min=v0,max=v11"`
	MaxBytes        int32                        `kafka:"min=v3,max=v11"`
	IsolationLevel  int8                         `kafka:"min=v4,max=v11"`
	SessionID       int32                        `kafka:"min=v7,max=v11"`
	SessionEpoch    int32                        `kafka:"min=v7,max=v11"`
	Topics          []FetchRequestTopic          `kafka:"min=v0,max=v11"`
	ForgottenTopics []FetchRequestForgottenTopic `kafka:"min=v7,max=v11"`
	RackID          string                       `kafka:"min=v11,max=v11"`
}

func (r *FetchRequest) ApiKey() ApiKey { return Fetch }

type FetchRequestTopic struct {
	Topic      string                  `kafka:"min=v0,max=v11"`
	ErrorCode  int16                   `kafka:"min=v7,max=v11"`
	SessionID  int32                   `kafka:"min=v7,max=v11"`
	Partitions []FetchRequestPartition `kafka:"min=v0,max=v11"`
}

type FetchRequestPartition struct {
	Partition          int32 `kafka:"min=v0,max=v11"`
	CurrentLeaderEpoch int32 `kafka:"min=v9,max=v11"`
	FetchOffset        int64 `kafka:"min=v0,max=v11"`
	LogStartOffset     int64 `kafka:"min=v5,max=v11"`
	PartitionMaxBytes  int32 `kafka:"min=v0,max=v11"`
}

type FetchRequestForgottenTopic struct {
	Topic      string  `kafka:"min=v7,max=v11"`
	Partitions []int32 `kafka:"min=v7,max=v11"`
}

type FetchResponse struct {
	ThrottleTimeMs int32                `kafka:"min=v1,max=v11"`
	Topics         []FetchResponseTopic `kafka:"min=v0,max=v11"`
}

func (r *FetchResponse) ApiKey() ApiKey { return Fetch }

func (r *FetchResponse) Close() error {
	for i := range r.Topics {
		t := &r.Topics[i]
		for j := range t.Partitions {
			p := &t.Partitions[j]
			p.RecordSet.Close()
		}
	}
	return nil
}

type FetchResponseTopic struct {
	Topic      string                   `kafka:"min=v0,max=v11"`
	Partitions []FetchResponsePartition `kafka:"min=v0,max=v11"`
}

type FetchResponsePartition struct {
	Partition            int32                      `kafka:"min=v0,max=v11"`
	ErrorCode            int16                      `kafka:"min=v0,max=v11"`
	HighWatermark        int64                      `kafka:"min=v0,max=v11"`
	LastStableOffset     int64                      `kafka:"min=v4,max=v11"`
	LogStartOffset       int64                      `kafka:"min=v5,max=v11"`
	AbortedTransactions  []FetchResponseTransaction `kafka:"min=v4,max=v11"`
	PreferredReadReplica int32                      `kafka:"min=v11,max=v11"`
	RecordSet            RecordSet                  `kafka:"min=v0,max=v11"`
}

type FetchResponseTransaction struct {
	ProducerID  int64 `kafka:"min=v4,max=v11"`
	FirstOffset int64 `kafka:"min=v4,max=v11"`
}
