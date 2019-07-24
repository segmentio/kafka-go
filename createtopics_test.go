package kafka

import (
	"testing"
)

func TestCreateTopicsResponseV0(t *testing.T) {
	testProtocolType(t,
		&createTopicsResponseV0{
			TopicErrors: []createTopicsResponseV0TopicError{{
				Topic:     "topic",
				ErrorCode: 2,
			}},
		},
		&createTopicsResponseV0{},
	)
}
