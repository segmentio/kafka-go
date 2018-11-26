package kafka

// A commit represents the instruction of publishing an update of the last
// offset read by a program for a topic and partition.
type commit struct {
	topic     string
	partition int
	attempt   int
	offset    int64
}

// makeCommit builds a commit value from a message, the resulting commit takes
// its topic, partition, attempt, and offset from the message.
func makeCommit(msg Message) commit {
	return commit{
		topic:     msg.Topic,
		partition: msg.Partition,
		attempt:   msg.Attempt,
		offset:    msg.Offset + 1,
	}
}

// makeCommits generates a slice of commits from a list of messages, it extracts
// the topic, partition, and offset of each message and builds the corresponding
// commit slice.
func makeCommits(msgs ...Message) []commit {
	commits := make([]commit, len(msgs))

	for i, m := range msgs {
		commits[i] = makeCommit(m)
	}

	return commits
}

// commitRequest is the data type exchanged between the CommitMessages method
// and internals of the reader's implementation.
type commitRequest struct {
	commits []commit
	errch   chan<- error
}

// makeOffsetsFromCommits constructs a map of partitions to offsets from a list
// of commits. Only commits with a non-zero attempt number are considered
// because they represent messages that have already been requeued, and this
// function is called when we need to generate the list of offsets to remove
// from the offset store used by the requeuing mechanism of the reader.
func makeOffsetsFromCommits(commits []commit) map[partition][]offset {
	var offsets map[partition][]offset

	for _, c := range commits {
		if c.attempt == 0 {
			continue
		}

		pkey := partition{
			topic:  c.topic,
			number: c.partition,
		}

		if offsets == nil {
			offsets = make(map[partition][]offset)
		}

		offsets[pkey] = append(offsets[pkey], offset{
			value: c.offset,
		})
	}

	return offsets
}
