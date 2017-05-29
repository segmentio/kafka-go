package kafka

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type apiKey int16

const (
	produceRequest          apiKey = 0
	fetchRequest            apiKey = 1
	offsetRequest           apiKey = 2
	metadataRequest         apiKey = 3
	offsetCommitRequest     apiKey = 8
	offsetFetchRequest      apiKey = 9
	groupCoordinatorRequest apiKey = 10
	joinGroupRequest        apiKey = 11
	heartbeatRequest        apiKey = 12
	leaveGroupRequest       apiKey = 13
	syncGroupRequest        apiKey = 14
	describeGroupsRequest   apiKey = 15
	listGroupsRequest       apiKey = 16
)

type apiVersion int16

const (
	v0 apiVersion = 0
	v1 apiVersion = 1
	v2 apiVersion = 2
)

type requestHeader struct {
	Size          int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	ClientID      string
}

// Message types
type messageSet []messageSetItem

type messageSetItem struct {
	Offset      int64
	MessageSize int32
	Message     message
}

type message struct {
	CRC        int32
	MagicByte  int8
	Attributes int8
	Timestamp  int64
	Key        []byte
	Value      []byte
}

func (m message) crc32() int32 {
	sum := uint32(0)
	sum = crc32Int8(sum, m.MagicByte)
	sum = crc32Int8(sum, m.Attributes)
	sum = crc32Int64(sum, m.Timestamp)
	sum = crc32Bytes(sum, m.Key)
	sum = crc32Bytes(sum, m.Value)
	return int32(sum)
}

func crc32Int8(sum uint32, v int8) uint32 {
	b := [1]byte{byte(v)}
	return crc32Add(sum, b[:])
}

func crc32Int32(sum uint32, v int32) uint32 {
	b := [4]byte{}
	binary.BigEndian.PutUint32(b[:], uint32(v))
	return crc32Add(sum, b[:])
}

func crc32Int64(sum uint32, v int64) uint32 {
	b := [8]byte{}
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return crc32Add(sum, b[:])
}

func crc32Bytes(sum uint32, b []byte) uint32 {
	if b == nil {
		sum = crc32Int32(sum, -1)
	} else {
		sum = crc32Int32(sum, int32(len(b)))
	}
	return crc32Add(sum, b)
}

func crc32Add(sum uint32, b []byte) uint32 {
	return crc32.Update(sum, crc32.IEEETable, b[:])
}

// Metadata API (v0)
type topicMetadataRequestV0 []string

type metadataResponseV0 struct {
	Brokers []brokerMetadataV0
	Topics  []topicMetadataV0
}

type brokerMetadataV0 struct {
	NodeID int32
	Host   string
	Port   int32
}

type topicMetadataV0 struct {
	TopicErrorCode int16
	TopicName      string
	Partitions     []partitionMetadataV0
}

type partitionMetadataV0 struct {
	PartitionErrorCode int16
	PartitionID        int32
	Leader             int32
	Replicas           []int32
	Isr                []int32
}

// Produce API (v2)
type produceRequestV2 struct {
	RequiredAcks int16
	Timeout      int32
	Topics       []produceRequestTopicV2
}

type produceRequestTopicV2 struct {
	TopicName  string
	Partitions []produceRequestPartitionV2
}

type produceRequestPartitionV2 struct {
	Partition      int32
	MessageSetSize int32
	MessageSet     messageSet
}

type produceResponseV2 struct {
	Topics       []produceResponseTopicV2
	ThrottleTime int32
}

type produceResponseTopicV2 struct {
	TopicName  string
	Partitions []produceResponsePartitionV2
}

type produceResponsePartitionV2 struct {
	Partition int32
	ErrorCode int16
	Offset    int64
	Timestamp int64
}

// Fetch API (v1)
type fetchRequestV1 struct {
	ReplicaID   int32
	MaxWaitTime int32
	MinBytes    int32
	Topics      []fetchRequestTopicV1
}

type fetchRequestTopicV1 struct {
	TopicName  string
	Partitions []fetchRequestPartitionV1
}

type fetchRequestPartitionV1 struct {
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
}

type fetchResponseV1 struct {
	ThrottleTime int32
	Topics       []fetchResponseTopicV1
}

type fetchResponseTopicV1 struct {
	TopicName  string
	Partitions []fetchResponsePartitionV1
}

type fetchResponsePartitionV1 struct {
	Partition           int32
	ErrorCode           int16
	HighwaterMarkOffset int64
	MessageSetSize      int32
	MessageSet          messageSet
}

// Offset API (v1)
type listOffsetRequestV1 struct {
	ReplicaID int32
	Topics    []listOffsetRequestTopicV1
}

type listOffsetRequestTopicV1 struct {
	TopicName  string
	Partitions []listOffsetRequestPartitionV1
}

type listOffsetRequestPartitionV1 struct {
	Partition int32
	Time      int64
}

type listOffsetResponseV1 struct {
	TopicName        string
	PartitionOffsets []partitionOffsetV1
}

type partitionOffsetV1 struct {
	Partition int32
	ErrorCode int16
	Timestamp int64
	Offset    int64
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
