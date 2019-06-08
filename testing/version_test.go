package kafka

import (
	"testing"
)

func TestSemVersionAtLeastEmpty(t *testing.T) {
	result := semver([]int{}).atLeast(semver([]int{1, 2}))
	if result {
		t.Errorf("Empty version can't be at least 1.2")
	}
}

func TestSemVersionAtLeastShorter(t *testing.T) {
	result := semver([]int{1, 1}).atLeast(semver([]int{1, 1, 2}))
	if result {
		t.Errorf("Version 1.1 version can't be at least 1.1.2")
	}
}
