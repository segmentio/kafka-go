package kafka

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
// command line in order to target a particular kafka version.
var kafkaVersion = parseVersion(os.Getenv("KAFKA_VERSION"))

// KafkaIsAtLeast returns true when the test broker is running a protocol
// version that is semver or newer.  It determines the broker's version using
// the `KAFKA_VERSION` environment variable.  If the var is unset, then this
// function will return true.
func KafkaIsAtLeast(semver string) bool {
	return kafkaVersion.atLeast(parseVersion(semver))
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
