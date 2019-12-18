package kafka

type (
	wildcardConfig struct {
		subscriberID              string
		assignments               wildcardAssignments
		updateChan                <-chan map[string][]int
		unsubscribeChan           chan<- string
		cancelUpdateTopicLoopChan chan struct{}
	}
	wildcardAssignments map[string]map[int]int64
)

func getOffsetsByPartitionByTopic(topics map[string][]int) (offsetsByPartitionByTopic wildcardAssignments) {

	offsetsByPartitionByTopic = map[string]map[int]int64{}
	for topic, partitions := range topics {
		offsetsByPartition := map[int]int64{}
		for _, partition := range partitions {
			offsetsByPartition[partition] = FirstOffset
		}
		offsetsByPartitionByTopic[topic] = offsetsByPartition
	}
	return
}
