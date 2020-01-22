package protocol

type ProduceRequest struct {
	TransactionalID string                `kafka:"min=v3,max=v8,nullable"`
	Acks            int16                 `kafka:"min=v0,max=v8"`
	Timeout         int32                 `kafka:"min=v0,max=v8"`
	Topics          []ProduceRequestTopic `kafka:"min=v0,max=v8"`
}

func (r *ProduceRequest) ApiKey() ApiKey { return Produce }

func (r *ProduceRequest) Close() error {
	for i := range r.Topics {
		t := &r.Topics[i]
		for j := range t.Partitions {
			p := &t.Partitions[j]
			p.RecordSet.Close()
		}
	}
	return nil
}

type ProduceRequestTopic struct {
	Topic      string                    `kafka:"min=v0,max=v8"`
	Partitions []ProduceRequestPartition `kafka:"min=v0,max=v8"`
}

type ProduceRequestPartition struct {
	Partition int32     `kafka:"min=v0,max=v8"`
	RecordSet RecordSet `kafka:"min=v0,max=v8"`
}

type ProduceResponse struct {
	Topics         []ProduceResponseTopic `kafka:"min=v0,max=v8"`
	ThrottleTimeMs int32                  `kafka:"min=v1,max=v8"`
}

func (r *ProduceResponse) ApiKey() ApiKey { return Produce }

type ProduceResponseTopic struct {
	Topic      string                     `kafka:"min=v0,max=v8"`
	Partitions []ProduceResponsePartition `kafka:"min=v0,max=v8"`
}

type ProduceResponsePartition struct {
	Partition      int32                  `kafka:"min=v0,max=v8"`
	ErrorCode      int16                  `kafka:"min=v0,max=v8"`
	BaseOffset     int64                  `kafka:"min=v0,max=v8"`
	LogAppendTime  int64                  `kafka:"min=v2,max=v8"`
	LogStartOffset int64                  `kafka:"min=v5,max=v8"`
	RecordErrors   []ProduceResponseError `kafka:"min=v8,max=v8"`
	ErrorMessage   string                 `kafka:"min=v8,max=v8,nullable"`
}

type ProduceResponseError struct {
	BatchIndex             int32  `kafka:"min=v8,max=v8"`
	BatchIndexErrorMessage string `kafka:"min=v8,max=v8,nullable"`
}
