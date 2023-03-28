package deletegroups_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/deletegroups"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

func TestDeleteGroupsRequest(t *testing.T) {
	for _, version := range []int16{0, 1, 2} {
		prototest.TestRequest(t, version, &deletegroups.Request{
			GroupIDs: []string{"group1", "group2"},
		})
	}
}

func TestDeleteGroupsResponse(t *testing.T) {
	for _, version := range []int16{0, 1, 2} {
		prototest.TestResponse(t, version, &deletegroups.Response{
			Responses: []deletegroups.ResponseGroup{
				{
					GroupID:   "group1",
					ErrorCode: 0,
				},
				{
					GroupID:   "group2",
					ErrorCode: 1,
				},
			},
		})
	}
}
