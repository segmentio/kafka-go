package kafka_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// ExampleCooperativeStickyAssignor shows how to use the CooperativeStickyAssignor
// for sticky partition assignment without rack affinity.
//
// This assignor is ideal when:
// - You want to minimize partition movement during rebalances
// - You don't need rack/AZ-aware assignment
// - You want compatibility with Sarama and Java Kafka clients
func ExampleCooperativeStickyAssignor() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka:9092"},
		GroupID: "my-group",
		Topic:   "my-topic",
		GroupBalancers: []kafka.GroupBalancer{
			&kafka.CooperativeStickyAssignor{},
		},
	})

	r.ReadMessage(context.Background())

	r.Close()
}

// ExampleCooperativeStickyAssignor_withConsumerGroup shows how to use
// CooperativeStickyAssignor with a ConsumerGroup for multi-topic consumption.
func ExampleCooperativeStickyAssignor_withConsumerGroup() {
	group, _ := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      "my-consumer-group",
		Brokers: []string{"kafka:9092"},
		Topics:  []string{"topic-a", "topic-b", "topic-c"},
		GroupBalancers: []kafka.GroupBalancer{
			&kafka.CooperativeStickyAssignor{},
		},
	})
	defer group.Close()

	ctx := context.Background()
	gen, _ := group.Next(ctx)

	// Process messages from assigned partitions
	for topic, partitions := range gen.Assignments {
		for _, partition := range partitions {
			// Start processing each partition
			_ = topic
			_ = partition
		}
	}

	// Commit offsets when done
	gen.CommitOffsets(map[string]map[int]int64{
		"topic-a": {0: 100},
	})
}

// ExampleCooperativeStickyRackAffinityAssignor shows how to use the
// CooperativeStickyRackAffinityAssignor for sticky partition assignment
// with rack/AZ-aware locality optimization.
//
// This assignor is ideal when:
// - You're running in AWS MSK or any multi-AZ Kafka deployment
// - You want to minimize cross-AZ data transfer costs
// - You want to minimize partition movement during rebalances
// - You want both sticky assignment AND rack affinity
//
// The Rack field should be set to match the broker's rack configuration.
// In AWS, this is typically the availability zone (e.g., "us-east-1a").
func ExampleCooperativeStickyRackAffinityAssignor() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka:9092"},
		GroupID: "my-group",
		Topic:   "my-topic",
		GroupBalancers: []kafka.GroupBalancer{
			&kafka.CooperativeStickyRackAffinityAssignor{
				Rack: findRackForCooperativeSticky(),
			},
		},
	})

	r.ReadMessage(context.Background())

	r.Close()
}

// ExampleCooperativeStickyRackAffinityAssignor_msk shows the recommended setup
// for Amazon MSK deployments. This configuration:
// - Minimizes cross-AZ data transfer (cost savings)
// - Maintains sticky assignments across rebalances (stability)
// - Falls back gracefully if rack info is unavailable
func ExampleCooperativeStickyRackAffinityAssignor_msk() {
	// MSK brokers are configured with rack = AZ ID (e.g., "use1-az1")
	// Consumer should be configured with the same AZ ID
	rack := findRackForCooperativeSticky()

	group, _ := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      "msk-consumer-group",
		Brokers: []string{"b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9092"},
		Topics:  []string{"orders", "events", "notifications"},
		GroupBalancers: []kafka.GroupBalancer{
			// Primary: sticky + rack affinity for optimal performance
			&kafka.CooperativeStickyRackAffinityAssignor{Rack: rack},
			// Fallback: sticky without rack (if rack info unavailable)
			&kafka.CooperativeStickyAssignor{},
			// Final fallback: range (for compatibility)
			kafka.RangeGroupBalancer{},
		},
	})
	defer group.Close()

	ctx := context.Background()
	for {
		gen, err := group.Next(ctx)
		if err != nil {
			break
		}

		// Process messages from each assigned partition
		for topic, partitions := range gen.Assignments {
			for _, partition := range partitions {
				// Process partition...
				_ = topic
				_ = partition
			}
		}

		// Commit offsets periodically
		gen.CommitOffsets(map[string]map[int]int64{})
	}
}

// ExampleCooperativeStickyRackAffinityAssignor_kubernetes shows setup for
// Kubernetes deployments where pods may be spread across availability zones.
func ExampleCooperativeStickyRackAffinityAssignor_kubernetes() {
	// In Kubernetes, you can inject the node's AZ via downward API:
	// env:
	//   - name: NODE_AZ
	//     valueFrom:
	//       fieldRef:
	//         fieldPath: metadata.labels['topology.kubernetes.io/zone']
	rack := os.Getenv("NODE_AZ")
	if rack == "" {
		// Fallback to AWS metadata if not in K8s or label not set
		rack = findRackForCooperativeSticky()
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka:9092"},
		GroupID: "k8s-consumer-group",
		Topic:   "my-topic",
		GroupBalancers: []kafka.GroupBalancer{
			&kafka.CooperativeStickyRackAffinityAssignor{Rack: rack},
			&kafka.CooperativeStickyAssignor{}, // Fallback
		},
	})

	r.ReadMessage(context.Background())

	r.Close()
}

// ExampleCooperativeStickyRackAffinityAssignor_dynamicRackFunc shows how to use
// the RackFunc field for lazy/dynamic AZ discovery.
//
// Benefits of RackFunc:
// - AZ discovery is deferred until first JoinGroup (faster startup)
// - Works well in dynamic environments where AZ may not be known at init time
// - Allows for complex discovery logic (broker format detection, etc.)
func ExampleCooperativeStickyRackAffinityAssignor_dynamicRackFunc() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka:9092"},
		GroupID: "dynamic-rack-group",
		Topic:   "my-topic",
		GroupBalancers: []kafka.GroupBalancer{
			&kafka.CooperativeStickyRackAffinityAssignor{
				// RackFunc is called once when UserData() is first invoked
				// The result is cached for subsequent calls
				RackFunc: func() string {
					// Try Kubernetes downward API first
					if az := os.Getenv("NODE_AZ"); az != "" {
						return az
					}
					// Fall back to AWS metadata
					return findRackForCooperativeSticky()
				},
			},
		},
	})

	r.ReadMessage(context.Background())

	r.Close()
}

// ExampleCooperativeStickyRackAffinityAssignor_mskWithDynamicDiscovery shows
// an MSK-optimized setup with automatic AZ zone ID detection.
//
// MSK uses zone IDs (e.g., "use1-az1") rather than zone names (e.g., "us-east-1a").
// This example shows how to detect and use the appropriate format.
func ExampleCooperativeStickyRackAffinityAssignor_mskWithDynamicDiscovery() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9092"},
		GroupID: "msk-dynamic-group",
		Topic:   "my-topic",
		GroupBalancers: []kafka.GroupBalancer{
			&kafka.CooperativeStickyRackAffinityAssignor{
				RackFunc: func() string {
					// For MSK, use zone ID (e.g., "use1-az1") instead of zone name
					// This matches the broker's rack configuration
					return ec2AvailabilityZoneIDForSticky()
				},
			},
		},
	})

	r.ReadMessage(context.Background())

	r.Close()
}

// ec2AvailabilityZoneIDForSticky returns the zone ID (e.g., "use1-az1")
// which is what MSK uses for broker rack configuration
func ec2AvailabilityZoneIDForSticky() string {
	client := http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			DisableCompression: true,
			DisableKeepAlives:  true,
		},
	}

	// Try IMDSv2 first
	req, _ := http.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "21600")
	tokenResp, err := client.Do(req)
	if err == nil {
		defer tokenResp.Body.Close()
		token, _ := io.ReadAll(tokenResp.Body)

		// Request zone ID instead of zone name
		req, _ = http.NewRequest("GET", "http://169.254.169.254/latest/meta-data/placement/availability-zone-id", nil)
		req.Header.Set("X-aws-ec2-metadata-token", string(token))
		r, err := client.Do(req)
		if err == nil {
			defer r.Body.Close()
			b, _ := io.ReadAll(r.Body)
			return string(b)
		}
	}

	return ""
}

// findRackForCooperativeSticky resolves the current rack/AZ for use with
// CooperativeStickyRackAffinityAssignor. It supports:
// - ECS with task metadata endpoint
// - EC2 instances
// - Kubernetes with NODE_AZ environment variable
func findRackForCooperativeSticky() string {
	// Check Kubernetes first (fastest - just env var)
	if az := os.Getenv("NODE_AZ"); az != "" {
		return az
	}

	// Check ECS
	if os.Getenv("ECS_CONTAINER_METADATA_URI") != "" {
		return ecsAvailabilityZoneForSticky()
	}

	// Check EC2
	if isEC2() {
		return ec2AvailabilityZoneForSticky()
	}

	return ""
}

func isEC2() bool {
	for _, path := range [...]string{
		"/sys/devices/virtual/dmi/id/product_uuid",
		"/sys/hypervisor/uuid",
	} {
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		s := string(b)
		if strings.HasPrefix(s, "EC2") || strings.HasPrefix(s, "ec2") {
			return true
		}
	}
	return false
}

func ecsAvailabilityZoneForSticky() string {
	client := http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			DisableCompression: true,
			DisableKeepAlives:  true,
		},
	}
	r, err := client.Get(os.Getenv("ECS_CONTAINER_METADATA_URI") + "/task")
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

func ec2AvailabilityZoneForSticky() string {
	client := http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			DisableCompression: true,
			DisableKeepAlives:  true,
		},
	}

	// Try IMDSv2 first (more secure)
	req, _ := http.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "21600")
	tokenResp, err := client.Do(req)
	if err == nil {
		defer tokenResp.Body.Close()
		token, _ := io.ReadAll(tokenResp.Body)

		req, _ = http.NewRequest("GET", "http://169.254.169.254/latest/meta-data/placement/availability-zone", nil)
		req.Header.Set("X-aws-ec2-metadata-token", string(token))
		r, err := client.Do(req)
		if err == nil {
			defer r.Body.Close()
			b, _ := io.ReadAll(r.Body)
			return string(b)
		}
	}

	// Fallback to IMDSv1
	r, err := client.Get("http://169.254.169.254/latest/meta-data/placement/availability-zone")
	if err != nil {
		return ""
	}
	defer r.Body.Close()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return ""
	}
	return string(b)
}
