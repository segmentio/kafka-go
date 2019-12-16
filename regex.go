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

func getOffsetsByPartitionByTopic(topics map[string][]int) (offsetsByPartitionByTopic map[string]map[int]int64) {

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
