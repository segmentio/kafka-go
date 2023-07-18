package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go/protocol/describeacls"
)

// DescribeACLsRequest represents a request sent to a kafka broker to describe
// existing ACLs.
type DescribeACLsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	// List of filters to filter ACLs on.
	Filters []ACLFilter
}

type ACLFilter struct {
	ResourceTypeFilter        ResourceType
	ResourceNameFilter        string
	ResourcePatternTypeFilter PatternType
	PrincipalFilter           string
	HostFilter                string
	Operation                 ACLOperationType
	PermissionType            ACLPermissionType
}

// DescribeACLsResponse represents a response from a kafka broker to an ACL
// describe request.
type DescribeACLsResponse struct {
	// The amount of time that the broker throttled the request.
	Throttle time.Duration

	// Error that occurred while attempting to describe
	// the ACLs.
	//
	// The errors contain the kafka error code. Programs may use the standard
	// errors.Is function to test the error against kafka error codes.
	Error error

	// ACL resources returned from the describe request.
	Resources []ACLResource
}

type ACLResource struct {
	ResourceType ResourceType
	ResourceName string
	PatternType  PatternType
	ACLs         []ACLDescription
}

type ACLDescription struct {
	Principal      string
	Host           string
	Operation      ACLOperationType
	PermissionType ACLPermissionType
}

func (c *Client) DescribeACLs(ctx context.Context, req *DescribeACLsRequest) (*DescribeACLsResponse, error) {
	filters := make([]describeacls.ACLFilters, len(req.Filters))

	for filterIdx, filter := range req.Filters {
		filters[filterIdx] = describeacls.ACLFilters{
			ResourceTypeFilter:        int8(filter.ResourceTypeFilter),
			ResourceNameFilter:        filter.ResourceNameFilter,
			ResourcePatternTypeFilter: int8(filter.ResourcePatternTypeFilter),
			PrincipalFilter:           filter.PrincipalFilter,
			HostFilter:                filter.HostFilter,
			Operation:                 int8(filter.Operation),
			PermissionType:            int8(filter.PermissionType),
		}
	}

	m, err := c.roundTrip(ctx, req.Addr, &describeacls.Request{
		Filters: filters,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka.(*Client).DescribeACLs: %w", err)
	}

	res := m.(*describeacls.Response)
	resources := make([]ACLResource, len(res.Resources))

	for resourceIdx, respResource := range res.Resources {
		descriptions := make([]ACLDescription, len(respResource.ACLs))

		for descriptionIdx, respDescription := range respResource.ACLs {
			descriptions[descriptionIdx] = ACLDescription{
				Principal:      respDescription.Principal,
				Host:           respDescription.Host,
				Operation:      ACLOperationType(respDescription.Operation),
				PermissionType: ACLPermissionType(respDescription.PermissionType),
			}
		}

		resources[resourceIdx] = ACLResource{
			ResourceType: ResourceType(respResource.ResourceType),
			ResourceName: respResource.ResourceName,
			PatternType:  PatternType(respResource.PatternType),
			ACLs:         descriptions,
		}
	}

	ret := &DescribeACLsResponse{
		Throttle:  makeDuration(res.ThrottleTimeMs),
		Error:     makeError(res.ErrorCode, res.ErrorMessage),
		Resources: resources,
	}

	return ret, nil
}
