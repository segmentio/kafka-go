package kafka

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"reflect"
	"time"
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
	return discardN(r, sz, n)
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
	switch v := reflect.ValueOf(a).Elem(); v.Kind() {
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

func readMessageOffsetAndSize(r *bufio.Reader, sz int) (offset int64, size int32, remain int, err error) {
	if remain, err = readInt64(r, sz, &offset); err != nil {
		return
	}
	remain, err = readInt32(r, remain, &size)
	return
}

func readMessageHeader(r *bufio.Reader, sz int) (attributes int8, timestamp int64, remain int, err error) {
	var version int8
	// TCP is already taking care of ensuring data integrirty, no need to waste
	// resources doing it a second time so we just skip the message CRC.
	if remain, err = discardInt32(r, sz); err != nil {
		return
	}

	if remain, err = readInt8(r, remain, &version); err != nil {
		return
	}

	if remain, err = readInt8(r, remain, &attributes); err != nil {
		return
	}

	switch version {
	case 0:
	case 1:
		if remain, err = readInt64(r, remain, &timestamp); err != nil {
			return
		}
	default:
		err = fmt.Errorf("unsupported message version: %d", version)
	}

	return
}

func readMessageBytes(r *bufio.Reader, sz int, b []byte) (n int, remain int, err error) {
	var bytesLen int32
	var limit int
	var count int

	if remain, err = readInt32(r, sz, &bytesLen); err != nil {
		fmt.Println(err)
		return
	}

	if bytesLen > 0 {
		n = int(bytesLen)
	}

	if n > remain {
		limit = remain
	} else {
		limit = n
	}
	if limit > len(b) {
		limit = len(b)
	}

	count, err = io.ReadFull(r, b[:limit])
	remain -= count
	return
}

func readResponse(r *bufio.Reader, sz int, res interface{}) error {
	sz, err := read(r, sz, res)
	switch err.(type) {
	case Error:
		sz, err = discardN(r, sz, sz)
	}
	return expectZeroSize(sz, err)
}

func expectZeroSize(sz int, err error) error {
	if err == nil && sz != 0 {
		err = fmt.Errorf("reading a response left %d unread bytes", sz)
	}
	return err
}

func ignoreShortRead(sz int, err error) (int, error) {
	if err == errShortRead {
		err = nil
	}
	return sz, err
}

func streamArray(r *bufio.Reader, sz int, cb func(*bufio.Reader, int) (int, error)) (int, error) {
	var err error
	var len int32

	if sz, err = readInt32(r, sz, &len); err != nil {
		return sz, err
	}

	for n := int(len); n > 0; n-- {
		if sz, err = cb(r, sz); err != nil {
			break
		}
	}

	return sz, err
}

func streamString(r *bufio.Reader, sz int, cb func(*bufio.Reader, int, int16) (int, error)) (int, error) {
	var err error
	var len int16

	if sz, err = readInt16(r, sz, &len); err != nil {
		return sz, err
	}

	sz0 := sz
	sz1 := sz - int(len)

	if sz, err = cb(r, sz, len); err != nil {
		return sz, err
	}

	if sz != sz1 {
		return sz, fmt.Errorf("streaming callback of string of length %d consumed %d bytes", len, sz0-sz)
	}

	return sz, nil
}

func streamBytes(r *bufio.Reader, sz int, cb func(*bufio.Reader, int, int32) (int, error)) (int, error) {
	var err error
	var len int32

	if sz, err = readInt32(r, sz, &len); err != nil {
		return sz, err
	}

	sz0 := sz
	sz1 := sz - int(len)

	if sz, err = cb(r, sz, len); err != nil {
		return sz, err
	}

	if sz != sz1 {
		return sz, fmt.Errorf("streaming callback of string of length %d consumed %d bytes", len, sz0-sz)
	}

	return sz, nil
}

func discardN(r *bufio.Reader, sz int, n int) (int, error) {
	n, err := r.Discard(n)
	return sz - n, err
}

func discardInt8(r *bufio.Reader, sz int) (int, error) {
	return discardN(r, sz, 1)
}

func discardInt16(r *bufio.Reader, sz int) (int, error) {
	return discardN(r, sz, 2)
}

func discardInt32(r *bufio.Reader, sz int) (int, error) {
	return discardN(r, sz, 4)
}

func discardInt64(r *bufio.Reader, sz int) (int, error) {
	return discardN(r, sz, 8)
}

func discardString(r *bufio.Reader, sz int) (int, error) {
	return streamString(r, sz, func(r *bufio.Reader, sz int, len int16) (int, error) {
		return discardN(r, sz, int(len))
	})
}

func discardBytes(r *bufio.Reader, sz int) (int, error) {
	return streamBytes(r, sz, func(r *bufio.Reader, sz int, len int32) (int, error) {
		return discardN(r, sz, int(len))
	})
}

func discard(r *bufio.Reader, sz int, a interface{}) (int, error) {
	switch a.(type) {
	case int8:
		return discardInt8(r, sz)
	case int16:
		return discardInt16(r, sz)
	case int32:
		return discardInt32(r, sz)
	case int64:
		return discardInt64(r, sz)
	case string:
		return discardString(r, sz)
	case []byte:
		return discardBytes(r, sz)
	}
	switch v := reflect.ValueOf(a); v.Kind() {
	case reflect.Struct:
		return discardStruct(r, sz, v)
	case reflect.Slice:
		return discardSlice(r, sz, v)
	default:
		panic(fmt.Sprintf("unsupported type: %T", a))
	}
}

func discardStruct(r *bufio.Reader, sz int, v reflect.Value) (int, error) {
	var err error
	for i, n := 0, v.NumField(); i != n; i++ {
		if sz, err = discard(r, sz, v.Field(i)); err != nil {
			break
		}
	}
	return sz, err
}

func discardSlice(r *bufio.Reader, sz int, v reflect.Value) (int, error) {
	var zero = reflect.Zero(v.Type().Elem())
	var err error
	var len int32

	if sz, err = readInt32(r, sz, &len); err != nil {
		return sz, err
	}

	for n := int(len); n > 0; n-- {
		if sz, err = discard(r, sz, zero); err != nil {
			break
		}
	}

	return sz, err
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

func timestamp() int64 {
	return timeToTimestamp(time.Now())
}

func timeToTimestamp(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func timestampToTime(t int64) time.Time {
	return time.Unix(t/1000, (t%1000)*int64(time.Millisecond))
}
