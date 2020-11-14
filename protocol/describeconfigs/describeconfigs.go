package describeconfigs

import "github.com/segmentio/kafka-go/protocol"

const (
	ResourceTypeBroker int8 = 4
	ResourceTypeTopic  int8 = 2
)

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	Resources            []RequestResource `kafka:"min=v0,max=v3"`
	IncludeSynonyms      bool              `kafka:"min=v1,max=v3"`
	IncludeDocumentation bool              `kafka:"min=v3,max=v3"`
}

type RequestResource struct {
	ResourceType int8     `kafka:"min=v0,max=v3"`
	ResourceName string   `kafka:"min=v0,max=v3"`
	ConfigNames  []string `kafka:"min=v0,max=v3"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.DescribeConfigs }

func (r *Request) Broker(cluster protocol.Cluster) (protocol.Broker, error) {
	return cluster.Brokers[cluster.Controller], nil
}

type Response struct {
	ThrottleTimeMs int32              `kafka:"min=v0,max=v3"`
	Resources      []ResponseResource `kafka:"min=v0,max=v3"`
}

type ResponseResource struct {
	ErrorCode    int16  `kafka:"min=v0,max=v3"`
	ErrorMessage string `kafka:"min=v0,max=v3,nullable"`
	ResourceType int8   `kafka:"min=v0,max=v3"`
	ResourceName string `kafka:"min=v0,max=v3"`

	ConfigEntries []ResponseConfigEntry `kafka:"min=v0,max=v3"`
}

type ResponseConfigEntry struct {
	ConfigName   string `kafka:"min=v0,max=v3"`
	ConfigValue  string `kafka:"min=v0,max=v3,nullable"`
	ReadOnly     bool   `kafka:"min=v0,max=v3"`
	ConfigSource int8   `kafka:"min=v0,max=v3"`
	IsSensitive  bool   `kafka:"min=v0,max=v3"`

	ConfigSynonyms []ResponseConfigSynonym `kafka:"min=v1,max=v3"`

	ConfigType          int8   `kafka:"min=v3,max=v3"`
	ConfigDocumentation string `kafka:"min=v3,max=v3,nullable"`
}

type ResponseConfigSynonym struct {
	ConfigName   string `kafka:"min=v1,max=v3"`
	ConfigValue  string `kafka:"min=v1,max=v3,nullable"`
	ConfigSource int8   `kafka:"min=v1,max=v3"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.DescribeConfigs }
