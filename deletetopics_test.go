package kafka

import (
	"testing"
)

func TestDeleteTopicsResponseV1(t *testing.T) {
	testProtocolType(t,
		&deleteTopicsResponseV0{
			TopicErrorCodes: []deleteTopicsResponseV0TopicErrorCode{{
				Topic:     "a",
				ErrorCode: 7,
			}},
		},
		&deleteTopicsResponseV0{},
	)
}
