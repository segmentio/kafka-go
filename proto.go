package kafka

import (
	"bufio"
	"encoding/binary"
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

	if m.Key == nil {
		binary.BigEndian.PutUint32(buf[:4], 0xFFFFFFFF) // -1
	} else {
		binary.BigEndian.PutUint32(buf[:4], uint32(len(m.Key)))
	}
	sum = crc32.Update(sum, crc32.IEEETable, buf[:4])
	sum = crc32.Update(sum, crc32.IEEETable, m.Key)

	binary.BigEndian.PutUint32(buf[:4], uint32(len(m.Value)))
	sum = crc32.Update(sum, crc32.IEEETable, buf[:4])
	sum = crc32.Update(sum, crc32.IEEETable, m.Value)

	return int32(sum)
}

// Metadata API
type topicMetadataRequest []string

type metadataResponse struct {
	Brokers []broker
	Topics  []topicMetadata
}

type broker struct {
	NodeID int32
	Host   string
	Port   int32
}

type topicMetadata struct {
	TopicErrorCode int16
	TopicName      string
	Partitions     []partitionMetadata
}

type partitionMetadata struct {
	PartitionErrorCode int16
	PartitionID        int32
	Leader             int32
	Replicas           []int32
	Isr                []int32
}

// Produce API
type produceRequestType struct {
	RequiredAcks int16
	Timeout      int32
	Topics       []produceRequestTopic
}

type produceRequestTopic struct {
	TopicName  string
	Partitions []produceRequestPartition
}

type produceRequestPartition struct {
	Partition      int32
	MessageSetSize int32
	MessageSet     messageSet
}

type produceResponse struct {
	Topics       []produceResponseTopic
	ThrottleTime int32
}

type produceResponseTopic struct {
	TopicName  string
	Partitions []produceResponsePartition
}

type produceResponsePartition struct {
	Partition int32
	ErrorCode int16
	Offset    int64
	Timestamp int64
}

// Offset API
type listOffsetRequest struct {
	ReplicaID int32
	Topics    []listOffsetRequestTopic
}

type listOffsetRequestTopic struct {
	TopicName  string
	Partitions []listOffsetRequestPartition
}

type listOffsetRequestPartition struct {
	Partition int32
	Time      int64
}

type listOffsetResponse struct {
	TopicName        string
	PartitionOffsets []partitionOffset
}

type partitionOffset struct {
	Partition int32
	ErrorCode int16
	Timestamp int64
	Offsets   []int64
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

func peekRead(r *bufio.Reader, n int, f func([]byte)) error {
	b, err := r.Peek(n)
	if err != nil {
		return err
	}
	f(b)
	_, err = r.Discard(n)
	return err
}

func readInt8(r *bufio.Reader, v *int8) error {
	return peekRead(r, 1, func(b []byte) { *v = makeInt8(b) })
}

func readInt16(r *bufio.Reader, v *int16) error {
	return peekRead(r, 2, func(b []byte) { *v = makeInt16(b) })
}

func readInt32(r *bufio.Reader, v *int32) error {
	return peekRead(r, 4, func(b []byte) { *v = makeInt32(b) })
}

func readInt64(r *bufio.Reader, v *int64) error {
	return peekRead(r, 8, func(b []byte) { *v = makeInt64(b) })
}

func readString(r *bufio.Reader, v *string) error {
	var b []byte
	var n int16

	if err := readInt16(r, &n); err != nil {
		return err
	}

	if n >= 0 {
		b = make([]byte, int(n))
		if _, err := io.ReadFull(r, b); err != nil {
			return err
		}
	}

	*v = string(b)
	return nil
}

func readBytes(r *bufio.Reader, v *[]byte) error {
	var b []byte
	var n int32

	if err := readInt32(r, &n); err != nil {
		return err
	}

	if n >= 0 {
		b = make([]byte, int(n))
		if _, err := io.ReadFull(r, b); err != nil {
			return err
		}
	}

	*v = b
	return nil
}

func readResponse(r *bufio.Reader, size int32, res interface{}) error {
	// TODO: check size?
	return read(r, res)
}

func read(r *bufio.Reader, a interface{}) error {
	switch v := a.(type) {
	case *int8:
		return readInt8(r, v)
	case *int16:
		return readInt16(r, v)
	case *int32:
		return readInt32(r, v)
	case *int64:
		return readInt64(r, v)
	case *string:
		return readString(r, v)
	case *[]byte:
		return readBytes(r, v)
	}

	v := reflect.ValueOf(a)
	if v.IsNil() {
		v.Set(reflect.New(v.Type().Elem()))
	}

	switch v = v.Elem(); v.Kind() {
	case reflect.Struct:
		return readStruct(r, v)
	case reflect.Slice:
		return readSlice(r, v)
	default:
		panic(fmt.Sprintf("unsupported type: %T", a))
	}
}

func readStruct(r *bufio.Reader, v reflect.Value) error {
	for i, n := 0, v.NumField(); i != n; i++ {
		if err := read(r, v.Field(i).Addr().Interface()); err != nil {
			return err
		}
	}
	return nil
}

func readSlice(r *bufio.Reader, v reflect.Value) error {
	size := int32(0)

	if err := readInt32(r, &size); err != nil {
		return err
	}

	n := int(size)
	v.Set(reflect.MakeSlice(v.Type(), n, n))

	for i := 0; i != n; i++ {
		if err := read(r, v.Index(i).Addr().Interface()); err != nil {
			return err
		}
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

func writeRequest(w *bufio.Writer, hdr requestHeader, req interface{}) error {
	hdr.Size = (sizeof(hdr) + sizeof(req)) - 4
	write(w, hdr)
	write(w, req)
	return w.Flush()
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
