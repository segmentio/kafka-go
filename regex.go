package kafka

type (
	regexConfig struct {
		subscriberID              string
		assignments               regexAssignments
		updateChan                <-chan map[string][]int
		unsubscribeChan           chan<- string
		cancelUpdateTopicLoopChan chan struct{}
		interuptChan              chan struct{}
	}
	regexAssignments map[string]map[int]int64
)

func (r *regexConfig) getAssignmentPartitionsForTopic(topic string) *[]int {
	if offsetsByPartition, ok := r.assignments[topic]; ok {
		var partitions []int
		for partition, _ := range offsetsByPartition {
			partitions = append(partitions, partition)
		}
		return &partitions
	}
	return nil
}
