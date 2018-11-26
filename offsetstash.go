package kafka

// offsetStash holds offsets by topic/partition => offset
type offsetStash map[partition]int64

// merge updates the offsetStash with the offsets from the provided messages
func (stash offsetStash) mergeCommits(commits []commit) {
	for _, c := range commits {
		pkey := partition{
			topic:  c.topic,
			number: c.partition,
		}

		if offset := stash[pkey]; c.offset > offset {
			stash[pkey] = c.offset
		}
	}
}

func (stash offsetStash) mergeOffsets(offsets map[partition][]offset) {
	for partition, offsets := range offsets {
		if len(offsets) == 0 {
			continue
		}

		max := offsets[0].value

		for _, off := range offsets {
			if off.value > max {
				max = off.value
			}
		}

		// Same +1 logic is in makeCommits, to commit offsets we need to specify
		// the next valid offset.
		max++

		if offset := stash[partition]; max > offset {
			stash[partition] = max
		}
	}
}

// reset clears the contents of the offsetStash
func (stash offsetStash) reset() {
	for pkey := range stash {
		delete(stash, pkey)
	}
}
