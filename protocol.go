package kafka

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"reflect"
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

func makeMessage(timestamp int64, key []byte, value []byte) message {
	m := message{
		MagicByte: 1,
		Timestamp: timestamp,
		Key:       key,
		Value:     value,
	}
	m.CRC = m.crc32()
	return m
}

func (m message) crc32() int32 {
	sum := uint32(0)

	buf := [10]byte{}
	buf[0] = byte(m.MagicByte)
	buf[1] = byte(m.Attributes)
	binary.BigEndian.PutUint64(buf[2:], uint64(m.Timestamp))

	sum = crc32.Update(sum, crc32.IEEETable, buf[:])
	sum = crc32Bytes(sum, m.Key)
	sum = crc32Bytes(sum, m.Value)

	return int32(sum)
}

func crc32Bytes(sum uint32, b []byte) uint32 {
	buf := [4]byte{}
	if b == nil {
		binary.BigEndian.PutUint32(buf[:4], 0xFFFFFFFF) // -1
	} else {
		binary.BigEndian.PutUint32(buf[:4], uint32(len(b)))
	}
	sum = crc32.Update(sum, crc32.IEEETable, buf[:4])
	sum = crc32.Update(sum, crc32.IEEETable, b)
	return sum
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
	TopicName string
	Partition []fetchResponsePartitionV1
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

var (
	errShortRead = errors.New("not enough bytes available to load the response")
)

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

func peekRead(r *bufio.Reader, sz int, n int, f func([]byte)) (int, error) {
	if n > sz {
		return sz, errShortRead
	}
	b, err := r.Peek(n)
	if err != nil {
		return sz, err
	}
	f(b)
	r.Discard(n)
	return sz - n, nil
}

func readInt8(r *bufio.Reader, sz int, v *int8) (int, error) {
	return peekRead(r, sz, 1, func(b []byte) { *v = makeInt8(b) })
}

func readInt16(r *bufio.Reader, sz int, v *int16) (int, error) {
	return peekRead(r, sz, 2, func(b []byte) { *v = makeInt16(b) })
}

func readInt32(r *bufio.Reader, sz int, v *int32) (int, error) {
	return peekRead(r, sz, 4, func(b []byte) { *v = makeInt32(b) })
}

func readInt64(r *bufio.Reader, sz int, v *int64) (int, error) {
	return peekRead(r, sz, 8, func(b []byte) { *v = makeInt64(b) })
}

func readString(r *bufio.Reader, sz int, v *string) (int, error) {
	var err error
	var len int16
	var b []byte

	if sz, err = readInt16(r, sz, &len); err != nil {
		return sz, err
	}

	if n := int(len); n >= 0 {
		if n > sz {
			return sz, errShortRead
		}
		b = make([]byte, n)
		if _, err = io.ReadFull(r, b); err != nil {
			return sz, err
		}
		sz -= n
	}

	*v = string(b)
	return sz, nil
}

func readBytes(r *bufio.Reader, sz int, v *[]byte) (int, error) {
	var err error
	var len int32
	var b []byte

	if sz, err = readInt32(r, sz, &len); err != nil {
		return sz, err
	}

	if n := int(len); n >= 0 {
		if n > sz {
			return sz, errShortRead
		}
		b = make([]byte, n)
		if _, err = io.ReadFull(r, b); err != nil {
			return sz, err
		}
		sz -= n
	}

	*v = b
	return sz, nil
}

func read(r *bufio.Reader, sz int, a interface{}) (int, error) {
	switch v := a.(type) {
	case *int8:
		return readInt8(r, sz, v)
	case *int16:
		return readInt16(r, sz, v)
	case *int32:
		return readInt32(r, sz, v)
	case *int64:
		return readInt64(r, sz, v)
	case *string:
		return readString(r, sz, v)
	case *[]byte:
		return readBytes(r, sz, v)
	}

	v := reflect.ValueOf(a)
	if v.IsNil() {
		v.Set(reflect.New(v.Type().Elem()))
	}

	switch v = v.Elem(); v.Kind() {
	case reflect.Struct:
		return readStruct(r, sz, v)
	case reflect.Slice:
		return readSlice(r, sz, v)
	default:
		panic(fmt.Sprintf("unsupported type: %T", a))
	}
}

func readStruct(r *bufio.Reader, sz int, v reflect.Value) (int, error) {
	var err error
	for i, n := 0, v.NumField(); i != n; i++ {
		if sz, err = read(r, sz, v.Field(i).Addr().Interface()); err != nil {
			return sz, err
		}
	}
	return sz, nil
}

func readSlice(r *bufio.Reader, sz int, v reflect.Value) (int, error) {
	var err error
	var len int32

	if sz, err = readInt32(r, sz, &len); err != nil {
		return sz, err
	}

	if n := int(len); n < 0 {
		v.Set(reflect.Zero(v.Type()))
	} else {
		v.Set(reflect.MakeSlice(v.Type(), n, n))

		for i := 0; i != n; i++ {
			if sz, err = read(r, sz, v.Index(i).Addr().Interface()); err != nil {
				return sz, err
			}
		}
	}

	return sz, nil
}

func readResponse(r *bufio.Reader, sz int, res interface{}) error {
	n, err := read(r, sz, res)
	if err != nil {
		return err
	}
	if n != 0 {
		return fmt.Errorf("reading a response of size %d left %d unread bytes", sz, n)
	}
	return nil
}

func writeInt8(w *bufio.Writer, i int8) error {
	return w.WriteByte(byte(i))
}

func writeInt16(w *bufio.Writer, i int16) error {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(i))
	return writeSmallBuffer(w, b[:])
}

func writeInt32(w *bufio.Writer, i int32) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(i))
	return writeSmallBuffer(w, b[:])
}

func writeInt64(w *bufio.Writer, i int64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(i))
	return writeSmallBuffer(w, b[:])
}

func writeString(w *bufio.Writer, s string) error {
	if err := writeInt16(w, int16(len(s))); err != nil {
		return err
	}
	_, err := w.WriteString(s)
	return err
}

func writeBytes(w *bufio.Writer, b []byte) error {
	n := int32(len(b))
	if b == nil {
		n = -1
	}
	if err := writeInt32(w, n); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func writeMessageSet(w *bufio.Writer, mset messageSet) error {
	// message sets are not preceded by their size for some reason
	for _, m := range mset {
		if err := write(w, m); err != nil {
			return err
		}
	}
	return nil
}

func writeSmallBuffer(w *bufio.Writer, b []byte) error {
	// faster for short byte slices because it doesn't cause a memory allocation
	// because the byte slice may be passed to the underlying io.Writer.
	for _, c := range b {
		if err := w.WriteByte(c); err != nil {
			return err
		}
	}
	return nil
}

func write(w *bufio.Writer, a interface{}) error {
	switch v := a.(type) {
	case int8:
		return writeInt8(w, v)
	case int16:
		return writeInt16(w, v)
	case int32:
		return writeInt32(w, v)
	case int64:
		return writeInt64(w, v)
	case string:
		return writeString(w, v)
	case []byte:
		return writeBytes(w, v)
	case messageSet:
		return writeMessageSet(w, v)
	}
	switch v := reflect.ValueOf(a); v.Kind() {
	case reflect.Struct:
		return writeStruct(w, v)
	case reflect.Slice:
		return writeSlice(w, v)
	default:
		panic(fmt.Sprintf("unsupported type: %T", a))
	}
}

func writeStruct(w *bufio.Writer, v reflect.Value) error {
	for i, n := 0, v.NumField(); i != n; i++ {
		if err := write(w, v.Field(i).Interface()); err != nil {
			return err
		}
	}
	return nil
}

func writeSlice(w *bufio.Writer, v reflect.Value) error {
	n := v.Len()
	if err := writeInt32(w, int32(n)); err != nil {
		return err
	}
	for i := 0; i != n; i++ {
		if err := write(w, v.Index(i).Interface()); err != nil {
			return err
		}
	}
	return nil
}

func writeRequest(w *bufio.Writer, hdr requestHeader, req interface{}) error {
	hdr.Size = (sizeof(hdr) + sizeof(req)) - 4
	write(w, hdr)
	write(w, req)
	return w.Flush()
}

func sizeof(a interface{}) int32 {
	switch v := a.(type) {
	case int8:
		return 1
	case int16:
		return 2
	case int32:
		return 4
	case int64:
		return 8
	case string:
		return 2 + int32(len(v))
	case []byte:
		return 4 + int32(len(v))
	case messageSet:
		return sizeofMessageSet(v)
	}
	switch v := reflect.ValueOf(a); v.Kind() {
	case reflect.Struct:
		return sizeofStruct(v)
	case reflect.Slice:
		return sizeofSlice(v)
	default:
		panic(fmt.Sprintf("unsupported type: %T", a))
	}
}

func sizeofMessageSet(set messageSet) (size int32) {
	for _, msg := range set {
		size += sizeof(msg)
	}
	return
}

func sizeofStruct(v reflect.Value) (size int32) {
	for i, n := 0, v.NumField(); i != n; i++ {
		size += sizeof(v.Field(i).Interface())
	}
	return
}

func sizeofSlice(v reflect.Value) (size int32) {
	size = 4
	for i, n := 0, v.Len(); i != n; i++ {
		size += sizeof(v.Index(i).Interface())
	}
	return
}

func minInt64(ints ...int64) int64 {
	min := ints[0]
	for _, val := range ints[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func maxInt64(ints ...int64) int64 {
	max := ints[0]
	for _, val := range ints[1:] {
		if val > max {
			max = val
		}
	}
	return max
}
