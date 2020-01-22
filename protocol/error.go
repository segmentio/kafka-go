package protocol

import (
	"fmt"
)

const (
	ErrTruncated = Error("truncated")
)

type Error string

func (e Error) Error() string { return string(e) }

func errorf(msg string, args ...interface{}) error {
	return Error(fmt.Sprintf(msg, args...))
}
