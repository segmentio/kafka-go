package protocol

import (
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
)

// Message is an interface implemented by all request and response types of the
// kafka protocol.
//
// This interface is used mostly as a safe-guard to provide a compile-time check
// for values passed to functions dealing kafka message types.
type Message interface {
	ApiKey() ApiKey
}

type ApiKey int16

func (k ApiKey) String() string {
	if i := int(k); i >= 0 && i < len(apiNames) {
		return apiNames[i]
	}
	return strconv.Itoa(int(k))
}

func (k ApiKey) MinVersion() int16 { return k.apiType().minVersion() }

func (k ApiKey) MaxVersion() int16 { return k.apiType().maxVersion() }

func (k ApiKey) SelectVersion(minVersion, maxVersion int16) int16 {
	min := k.MinVersion()
	max := k.MaxVersion()
	switch {
	case min > maxVersion:
		return min
	case max < maxVersion:
		return max
	default:
		return maxVersion
	}
}

func (k ApiKey) apiType() apiType {
	if i := int(k); i >= 0 && i < len(apiTypes) {
		return apiTypes[i]
	}
	return apiType{}
}

const (
	Produce                     ApiKey = 0
	Fetch                       ApiKey = 1
	ListOffsets                 ApiKey = 2
	Metadata                    ApiKey = 3
	LeaderAndIsr                ApiKey = 4
	StopReplica                 ApiKey = 5
	UpdateMetadata              ApiKey = 6
	ControlledShutdown          ApiKey = 7
	OffsetCommit                ApiKey = 8
	OffsetFetch                 ApiKey = 9
	FindCoordinator             ApiKey = 10
	JoinGroup                   ApiKey = 11
	Heartbeat                   ApiKey = 12
	LeaveGroup                  ApiKey = 13
	SyncGroup                   ApiKey = 14
	DescribeGroups              ApiKey = 15
	ListGroups                  ApiKey = 16
	SaslHandshake               ApiKey = 17
	ApiVersions                 ApiKey = 18
	CreateTopics                ApiKey = 19
	DeleteTopics                ApiKey = 20
	DeleteRecords               ApiKey = 21
	InitProducerId              ApiKey = 22
	OffsetForLeaderEpoch        ApiKey = 23
	AddPartitionsToTxn          ApiKey = 24
	AddOffsetsToTxn             ApiKey = 25
	EndTxn                      ApiKey = 26
	WriteTxnMarkers             ApiKey = 27
	TxnOffsetCommit             ApiKey = 28
	DescribeAcls                ApiKey = 29
	CreateAcls                  ApiKey = 30
	DeleteAcls                  ApiKey = 31
	DescribeConfigs             ApiKey = 32
	AlterConfigs                ApiKey = 33
	AlterReplicaLogDirs         ApiKey = 34
	DescribeLogDirs             ApiKey = 35
	SaslAuthenticate            ApiKey = 36
	CreatePartitions            ApiKey = 37
	CreateDelegationToken       ApiKey = 38
	RenewDelegationToken        ApiKey = 39
	ExpireDelegationToken       ApiKey = 40
	DescribeDelegationToken     ApiKey = 41
	DeleteGroups                ApiKey = 42
	ElectLeaders                ApiKey = 43
	IncrementalAlterConfigs     ApiKey = 44
	AlterPartitionReassignments ApiKey = 45
	ListPartitionReassignments  ApiKey = 46
	OffsetDelete                ApiKey = 47

	numApis = 48
)

var apiNames = [numApis]string{
	Produce:                     "Produce",
	Fetch:                       "Fetch",
	ListOffsets:                 "ListOffsets",
	Metadata:                    "Metadata",
	LeaderAndIsr:                "LeaderAndIsr",
	StopReplica:                 "StopReplica",
	UpdateMetadata:              "UpdateMetadata",
	ControlledShutdown:          "ControlledShutdown",
	OffsetCommit:                "OffsetCommit",
	OffsetFetch:                 "OffsetFetch",
	FindCoordinator:             "FindCoordinator",
	JoinGroup:                   "JoinGroup",
	Heartbeat:                   "Heartbeat",
	LeaveGroup:                  "LeaveGroup",
	SyncGroup:                   "SyncGroup",
	DescribeGroups:              "DescribeGroups",
	ListGroups:                  "ListGroups",
	SaslHandshake:               "SaslHandshake",
	ApiVersions:                 "ApiVersions",
	CreateTopics:                "CreateTopics",
	DeleteTopics:                "DeleteTopics",
	DeleteRecords:               "DeleteRecords",
	InitProducerId:              "InitProducerId",
	OffsetForLeaderEpoch:        "OffsetForLeaderEpoch",
	AddPartitionsToTxn:          "AddPartitionsToTxn",
	AddOffsetsToTxn:             "AddOffsetsToTxn",
	EndTxn:                      "EndTxn",
	WriteTxnMarkers:             "WriteTxnMarkers",
	TxnOffsetCommit:             "TxnOffsetCommit",
	DescribeAcls:                "DescribeAcls",
	CreateAcls:                  "CreateAcls",
	DeleteAcls:                  "DeleteAcls",
	DescribeConfigs:             "DescribeConfigs",
	AlterConfigs:                "AlterConfigs",
	AlterReplicaLogDirs:         "AlterReplicaLogDirs",
	DescribeLogDirs:             "DescribeLogDirs",
	SaslAuthenticate:            "SaslAuthenticate",
	CreatePartitions:            "CreatePartitions",
	CreateDelegationToken:       "CreateDelegationToken",
	RenewDelegationToken:        "RenewDelegationToken",
	ExpireDelegationToken:       "ExpireDelegationToken",
	DescribeDelegationToken:     "DescribeDelegationToken",
	DeleteGroups:                "DeleteGroups",
	ElectLeaders:                "ElectLeaders",
	IncrementalAlterConfigs:     "IncrementalAlfterConfigs",
	AlterPartitionReassignments: "AlterPartitionReassignments",
	ListPartitionReassignments:  "ListPartitionReassignments",
	OffsetDelete:                "OffsetDelete",
}

type messageType struct {
	version int16
	gotype  reflect.Type
	decode  decodeFunc
	encode  encodeFunc
}

func (t *messageType) new() Message {
	return reflect.New(t.gotype).Interface().(Message)
}

type apiType struct {
	requests  []messageType
	responses []messageType
}

func (t apiType) minVersion() int16 {
	if len(t.requests) == 0 {
		return 0
	}
	return t.requests[0].version
}

func (t apiType) maxVersion() int16 {
	if len(t.requests) == 0 {
		return 0
	}
	return t.requests[len(t.requests)-1].version
}

var apiTypes [numApis]apiType

// Register is automatically called by sub-packages are imported to install a
// new pair of request/response message types.
func Register(req, res Message) {
	k1 := req.ApiKey()
	k2 := res.ApiKey()

	if k1 != k2 {
		panic(fmt.Sprintf("[%T/%T]: request and response API keys mismatch: %d != %d", req, res, k1, k2))
	}

	apiTypes[k1] = apiType{
		requests:  typesOf(req),
		responses: typesOf(res),
	}
}

func typesOf(v interface{}) []messageType {
	return makeTypes(reflect.TypeOf(v).Elem())
}

func makeTypes(t reflect.Type) []messageType {
	minVersion := int16(-1)
	maxVersion := int16(-1)

	forEachStructField(t, func(_ reflect.Type, _ index, tag string) {
		forEachStructTag(tag, func(tag structTag) bool {
			if minVersion < 0 || tag.MinVersion < minVersion {
				minVersion = tag.MinVersion
			}
			if maxVersion < 0 || tag.MaxVersion > maxVersion {
				maxVersion = tag.MaxVersion
			}
			return true
		})
	})

	types := make([]messageType, 0, (maxVersion-minVersion)+1)

	for v := minVersion; v <= maxVersion; v++ {
		types = append(types, messageType{
			version: v,
			gotype:  t,
			decode:  decodeFuncOf(t, v, structTag{}),
			encode:  encodeFuncOf(t, v, structTag{}),
		})
	}

	return types
}

type structTag struct {
	MinVersion int16
	MaxVersion int16
	Compact    bool
	Nullable   bool
}

func forEachStructTag(tag string, do func(structTag) bool) {
	if tag == "-" {
		return // special case to ignore the field
	}

	forEach(tag, '|', func(s string) bool {
		tag := structTag{
			MinVersion: -1,
			MaxVersion: -1,
		}

		var err error
		forEach(s, ',', func(s string) bool {
			switch {
			case strings.HasPrefix(s, "min="):
				tag.MinVersion, err = parseVersion(s[4:])
			case strings.HasPrefix(s, "max="):
				tag.MaxVersion, err = parseVersion(s[4:])
			case s == "compact":
				tag.Compact = true
			case s == "nullable":
				tag.Nullable = true
			default:
				err = fmt.Errorf("unrecognized option: %q", s)
			}
			return err == nil
		})

		if err != nil {
			panic(fmt.Errorf("malformed struct tag: %w", err))
		}

		if tag.MinVersion < 0 && tag.MaxVersion >= 0 {
			panic(fmt.Errorf("missing minimum version in struct tag: %q", s))
		}

		if tag.MaxVersion < 0 && tag.MinVersion >= 0 {
			panic(fmt.Errorf("missing maximum version in struct tag: %q", s))
		}

		if tag.MinVersion > tag.MaxVersion {
			panic(fmt.Errorf("invalid version range in struct tag: %q", s))
		}

		return do(tag)
	})
}

func forEach(s string, sep byte, do func(string) bool) bool {
	for len(s) != 0 {
		p := ""
		i := strings.IndexByte(s, sep)
		if i < 0 {
			p, s = s, ""
		} else {
			p, s = s[:i], s[i+1:]
		}
		if !do(p) {
			return false
		}
	}
	return true
}

func forEachStructField(t reflect.Type, do func(reflect.Type, index, string)) {
	for i, n := 0, t.NumField(); i < n; i++ {
		f := t.Field(i)

		if f.PkgPath != "" && f.Name != "_" {
			continue
		}

		kafkaTag, ok := f.Tag.Lookup("kafka")
		if !ok {
			kafkaTag = "|"
		}

		do(f.Type, indexOf(f), kafkaTag)
	}
}

func parseVersion(s string) (int16, error) {
	if !strings.HasPrefix(s, "v") {
		return 0, fmt.Errorf("invalid version number: %q", s)
	}
	i, err := strconv.ParseInt(s[1:], 10, 16)
	if err != nil {
		return 0, fmt.Errorf("invalid version number: %q: %w", s, err)
	}
	if i < 0 {
		return 0, fmt.Errorf("invalid negative version number: %q", s)
	}
	return int16(i), nil
}

func dontExpectEOF(err error) error {
	switch err {
	case nil:
		return nil
	case io.EOF:
		return io.ErrUnexpectedEOF
	default:
		return err
	}
}

type Broker struct {
	Rack string
	Host string
	Port int
	ID   int
}

func (b Broker) String() string {
	return net.JoinHostPort(b.Host, strconv.Itoa(b.Port))
}

func (b Broker) Format(w fmt.State, v rune) {
	switch v {
	case 'd':
		io.WriteString(w, strconv.Itoa(b.ID))
	case 's':
		io.WriteString(w, b.String())
	case 'v':
		io.WriteString(w, strconv.Itoa(b.ID))
		io.WriteString(w, " ")
		io.WriteString(w, b.String())
		if b.Rack != "" {
			io.WriteString(w, " ")
			io.WriteString(w, b.Rack)
		}
	}
}

type Topic struct {
	Name       string
	Error      int
	Partitions map[int]Partition
}

type Partition struct {
	ID       int
	Error    int
	Leader   int
	Replicas []int
	ISR      []int
	Offline  []int
}

// BrokerMessage is an extension of the Message interface implemented by some
// request types to customize the broker assignment logic.
type BrokerMessage interface {
	// Given a representation of the kafka cluster state as argument, returns
	// the broker that the message should be routed to.
	Broker(Cluster) (Broker, error)
}

// GroupMessage is an extension of the Message interface implemented by some
// request types to inform the program that they should be routed to a group
// coordinator.
type GroupMessage interface {
	// Returns the group configured on the message.
	Group() string
}

// PreparedMessage is an extension of the Message interface implemented by some
// request types which may need to run some pre-processing on their state before
// being sent.
type PreparedMessage interface {
	// Prepares the message before being sent to a kafka broker using the API
	// version passed as argument.
	Prepare(apiVersion int16)
}

// Splitter is an interface implemented by messages that can be split into
// multiple requests and have their results merged back by a Merger.
type Splitter interface {
	// For a given cluster layout, returns the list of messages constructed
	// from the receiver for each requests that should be sent to the cluster.
	// The second return value is a Merger which can be used to merge back the
	// results of each request into a single message (or an error).
	Split(Cluster) ([]Message, Merger, error)
}

// Merger is an interface implemented by messages which can merge multiple
// results into one response.
type Merger interface {
	// Given a list of message and associated results, merge them back into a
	// response (or an error). The results must be either Message or error
	// values, other types should trigger a panic.
	Merge(messages []Message, results []interface{}) (Message, error)
}

// Result converts r to a Message or and error, or panics if r could be be
// converted to these types.
func Result(r interface{}) (Message, error) {
	switch v := r.(type) {
	case Message:
		return v, nil
	case error:
		return nil, v
	default:
		panic(fmt.Errorf("BUG: result must be a message or an error but not %T", v))
	}
}
