package aws_msk_iam

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// The test cases in this file should be minimum as most of the test cases are covered by msk_iam_test.go,

func TestDefaultExpiry(t *testing.T) {
	expiry := time.Second * 5
	testCases := map[string]struct {
		Expiry time.Duration
	}{
		"with default":    {Expiry: expiry},
		"without default": {},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := defaultExpiry(testCase.Expiry)
			if testCase.Expiry == 0 {
				assert.Equal(t, time.Minute*5, actual)
			} else {
				assert.Equal(t, expiry, actual)
			}

		})
	}
}

func TestDefaultSignTime(t *testing.T) {
	testCases := map[string]struct {
		SignTime time.Time
	}{
		"with default":    {SignTime: signTime},
		"without default": {},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := defaultSignTime(testCase.SignTime)
			if testCase.SignTime.IsZero() {
				assert.True(t, actual.After(signTime))
			} else {
				assert.Equal(t, signTime, actual)
			}

		})
	}
}
