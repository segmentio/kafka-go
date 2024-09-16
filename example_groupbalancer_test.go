package kafka

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

// ExampleNewReader_rackAffinity shows how the RackAffinityGroupBalancer can be
// used to pair up consumers with brokers in the same AWS availability zone.
// This code assumes that each brokers' rack is configured to be the name of the
// AZ in which it is running.
func ExampleNewReader_rackAffinity() {
	r := NewReader(ReaderConfig{
		Brokers: []string{"kafka:9092"},
		GroupID: "my-group",
		Topic:   "my-topic",
		GroupBalancers: []GroupBalancer{
			RackAffinityGroupBalancer{Rack: findRack()},
			RangeGroupBalancer{},
		},
	})

	r.ReadMessage(context.Background())

	r.Close()
}

// findRack is the basic rack resolver strategy for use in AWS.  It supports
//   - ECS with the task metadata endpoint enabled (returns the container
//     instance's availability zone)
//   - Linux EC2 (returns the instance's availability zone)
func findRack() string {
	switch whereAmI() {
	case "ecs":
		return ecsAvailabilityZone()
	case "ec2":
		return ec2AvailabilityZone()
	}
	return ""
}

const ecsContainerMetadataURI = "ECS_CONTAINER_METADATA_URI"

// whereAmI determines which strategy the rack resolver should use.
func whereAmI() string {
	// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint.html
	if os.Getenv(ecsContainerMetadataURI) != "" {
		return "ecs"
	}
	// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/identify_ec2_instances.html
	for _, path := range [...]string{
		"/sys/devices/virtual/dmi/id/product_uuid",
		"/sys/hypervisor/uuid",
	} {
		b, err := ioutil.ReadFile(path)
		if err != nil {
			continue
		}
		s := string(b)
		switch {
		case strings.HasPrefix(s, "EC2"), strings.HasPrefix(s, "ec2"):
			return "ec2"
		}
	}
	return "somewhere"
}

// ecsAvailabilityZone queries the task endpoint for the metadata URI that ECS
// injects into the ECS_CONTAINER_METADATA_URI variable in order to retrieve
// the availability zone where the task is running.
func ecsAvailabilityZone() string {
	client := http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			DisableCompression: true,
			DisableKeepAlives:  true,
		},
	}
	r, err := client.Get(os.Getenv(ecsContainerMetadataURI) + "/task")
	if err != nil {
		return ""
	}
	defer r.Body.Close()

	var md struct {
		AvailabilityZone string
	}
	if err := json.NewDecoder(r.Body).Decode(&md); err != nil {
		return ""
	}
	return md.AvailabilityZone
}

// ec2AvailabilityZone queries the metadata endpoint to discover the
// availability zone where this code is running.  we avoid calling this function
// unless we know we're in EC2.  Otherwise, in other environments, we would need
// to wait for the request to 169.254.169.254 to timeout before proceeding.
func ec2AvailabilityZone() string {
	client := http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			DisableCompression: true,
			DisableKeepAlives:  true,
		},
	}
	r, err := client.Get("http://169.254.169.254/latest/meta-data/placement/availability-zone")
	if err != nil {
		return ""
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return ""
	}
	return string(b)
}
