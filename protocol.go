package kafka

import (
	"bufio"
	"encoding/binary"
	"fmt"
)

type apiKey int16

const (
	produceRequest          apiKey = 0
	fetchRequest                   = 1
	listOffsetRequest              = 2
	metadataRequest                = 3
	offsetCommitRequest            = 8
	offsetFetchRequest             = 9
	groupCoordinatorRequest        = 10
	joinGroupRequest               = 11
	heartbeatRequest               = 12
	leaveGroupRequest              = 13
	syncGroupRequest               = 14
	describeGroupsRequest          = 15
	listGroupsRequest              = 16
	createTopicsRequest            = 19
	deleteTopicsRequest            = 20
)

type apiVersion int16

const (
	v0 apiVersion = 0
	v1 apiVersion = 1
	v2 apiVersion = 2
	v3 apiVersion = 3
)

type requestHeader struct {
	Size          int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	ClientID      string
}

func (h requestHeader) size() int32 {
	return 4 + 2 + 2 + 4 + sizeofString(h.ClientID)
}

func (h requestHeader) writeTo(w *bufio.Writer) {
	writeInt32(w, h.Size)
	writeInt16(w, h.ApiKey)
	writeInt16(w, h.ApiVersion)
	writeInt32(w, h.CorrelationID)
	writeString(w, h.ClientID)
}

type request interface {
	size() int32
	writeTo(*bufio.Writer)
}

func makeInt8(b []byte) int8 {
	return int8(b[0])
}

func makeInt16(b []byte) int16 {
	return int16(binary.BigEndian.Uint16(b))
}

func makeInt32(b []byte) int32 {
	return int32(binary.BigEndian.Uint32(b))
}

func makeInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

func expectZeroSize(sz int, err error) error {
	if err == nil && sz != 0 {
		err = fmt.Errorf("reading a response left %d unread bytes", sz)
	}
	return err
}
