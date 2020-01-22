package protocol

import (
	"fmt"
)

const (
	ErrCorrupted = Error("corrupted")
	ErrTruncated = Error("truncated")
)

// Error is a string type implementing the error interface and used to declare
// constants representing recoverable protocol errors.
type Error string

func (e Error) Error() string { return string(e) }

func errorf(msg string, args ...interface{}) error {
	return Error(fmt.Sprintf(msg, args...))
}
