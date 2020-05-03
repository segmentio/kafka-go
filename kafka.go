package kafka

// Broker represents a kafka broker in a kafka cluster.
type Broker struct {
	Host string
	Port int
	ID   int
	Rack string
}

// Topic represents a topic in a kafka cluster.
type Topic struct {
	// Name of the topic.
	Name string

	// True if the topic is internal.
	Internal bool

	// The list of partition currently available on this topic.
	Partitions []Partition

	// An error that may have occured while attempting to read the topic
	// metadata.
	//
	// The error contains both the kafka error code, and an error message
	// returned by the kafka broker. Programs may use the standard errors.Is
	// function to test the error against kafka error codes.
	Error error
}

// Partition carries the metadata associated with a kafka partition.
type Partition struct {
	// Name of the topic that the partition belongs to, and its index in the
	// topic.
	Topic string
	ID    int

	// Leader, replicas, and ISR for the partition.
	Leader   Broker
	Replicas []Broker
	Isr      []Broker

	// An error that may have occured while attempting to read the partition
	// metadata.
	//
	// The error contains both the kafka error code, and an error message
	// returned by the kafka broker. Programs may use the standard errors.Is
	// function to test the error against kafka error codes.
	Error error
}
