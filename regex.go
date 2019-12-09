package kafka

type (
	regexConfig struct {
		partitions      []Partition
		updateChan      <-chan []Partition
		unsubscribeChan chan<- struct{}
	}
)
