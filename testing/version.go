package testing

import (
	"os"
	"strconv"
	"strings"
)

type semver []int

func (v semver) atLeast(other semver) bool {
	for i := range v {
		if i >= len(other) {
			break
		}
		if v[i] < other[i] {
			return false
		}
		if v[i] > other[i] {
			return true
		}
	}
	for i := len(v); i < len(other); i++ {
		if other[i] > 0 {
			return false
		}
	}
	return true
}

// kafkaVersion is set in the circle config.  It can also be provided on the
// command line in order to target a particular kafka version. Non-numeric
// values (e.g., "tansu-0.6.0") are tolerated and parsed as an empty semver
// so that init does not panic when running against alternative brokers — use
// IsTansu() to detect those cases.
var kafkaVersion = parseEnvKafkaVersion(os.Getenv("KAFKA_VERSION"))

func parseEnvKafkaVersion(v string) semver {
	if v == "" {
		return nil
	}
	for _, ch := range v {
		if ch != '.' && (ch < '0' || ch > '9') {
			return nil
		}
	}
	return parseVersion(v)
}

// KafkaIsAtLeast returns true when the test broker is running a protocol
// version that is semver or newer.  It determines the broker's version using
// the `KAFKA_VERSION` environment variable.  If the var is unset, then this
// function will return true.
func KafkaIsAtLeast(semver string) bool {
	return kafkaVersion.atLeast(parseVersion(semver))
}

// IsTansu reports whether tests are running against Tansu, a Kafka-compatible
// broker with some semantic differences from Apache Kafka:
//
//   - Enforces KIP-394 (MEMBER_ID_REQUIRED) across all JoinGroup versions,
//     not only v4+ as the KIP prescribes.
//   - Does not bump the consumer group generation_id on soft rejoin of an
//     existing dynamic member; it only increments when the group composition
//     actually changes. Apache Kafka bumps on every JoinGroup from an
//     existing member.
//
// Set `KAFKA_VERSION` to a value beginning with "tansu" (e.g., "tansu-0.6.0")
// to opt into Tansu-aware test branches.
func IsTansu() bool {
	return strings.HasPrefix(os.Getenv("KAFKA_VERSION"), "tansu")
}

func parseVersion(semver string) semver {
	if semver == "" {
		return nil
	}
	parts := strings.Split(semver, ".")
	version := make([]int, len(parts))
	for i := range version {
		v, err := strconv.Atoi(parts[i])
		if err != nil {
			// panic-ing because tests should be using hard-coded version values
			panic("invalid version string: " + semver)
		}
		version[i] = v
	}
	return version
}
