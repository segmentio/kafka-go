package kafka

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

type ApiVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

func (v ApiVersion) Format(w fmt.State, r rune) {
	switch r {
	case 's':
		fmt.Fprint(w, apiKey(v.ApiKey))
	case 'd':
		switch {
		case w.Flag('-'):
			fmt.Fprint(w, v.MinVersion)
		case w.Flag('+'):
			fmt.Fprint(w, v.MaxVersion)
		default:
			fmt.Fprint(w, v.ApiKey)
		}
	case 'v':
		switch {
		case w.Flag('-'):
			fmt.Fprintf(w, "v%d", v.MinVersion)
		case w.Flag('+'):
			fmt.Fprintf(w, "v%d", v.MaxVersion)
		case w.Flag('#'):
			fmt.Fprintf(w, "kafka.ApiVersion{ApiKey:%d MinVersion:%d MaxVersion:%d}", v.ApiKey, v.MinVersion, v.MaxVersion)
		default:
			fmt.Fprintf(w, "%s[v%d:v%d]", apiKey(v.ApiKey), v.MinVersion, v.MaxVersion)
		}
	}
}

type apiKey int16

const (
	produce apiKey = iota
	fetch
	listOffsets
	metadata
	leaderAndIsr
	stopReplica
	updateMetadata
	controlledShutdown
	offsetCommit
	offsetFetch
	findCoordinator
	joinGroup
	heartbeat
	leaveGroup
	syncGroup
	describeGroups
	listGroups
	saslHandshake
	apiVersions
	createTopics
	deleteTopics
	deleteRecords
	initProducerId
	offsetForLeaderEpoch
	addPartitionsToTxn
	addOffsetsToTxn
	endTxn
	writeTxnMarkers
	txnOffsetCommit
	describeAcls
	createAcls
	deleteAcls
	describeConfigs
	alterConfigs
	alterReplicaLogDirs
	describeLogDirs
	saslAuthenticate
	createPartitions
	createDelegationToken
	renewDelegationToken
	expireDelegationToken
	describeDelegationToken
	deleteGroups
	electLeaders
	incrementalAlterConfigs
	alterPartitionReassignments
	listPartitionReassignments
	offsetDelete
)

func (k apiKey) String() string {
	if i := int(k); i >= 0 && i < len(apiKeyStrings) {
		return apiKeyStrings[i]
	}
	return strconv.Itoa(int(k))
}

type apiVersion int16

const (
	v0 apiVersion = iota
	v1
	v2
	v3
	v4
	v5
	v6
	v7
	v8
	v9
	v10
)

var apiKeyStrings = [...]string{
	produce:                     "Produce",
	fetch:                       "Fetch",
	listOffsets:                 "ListOffsets",
	metadata:                    "Metadata",
	leaderAndIsr:                "LeaderAndIsr",
	stopReplica:                 "StopReplica",
	updateMetadata:              "UpdateMetadata",
	controlledShutdown:          "ControlledShutdown",
	offsetCommit:                "OffsetCommit",
	offsetFetch:                 "OffsetFetch",
	findCoordinator:             "FindCoordinator",
	joinGroup:                   "JoinGroup",
	heartbeat:                   "Heartbeat",
	leaveGroup:                  "LeaveGroup",
	syncGroup:                   "SyncGroup",
	describeGroups:              "DescribeGroups",
	listGroups:                  "ListGroups",
	saslHandshake:               "SaslHandshake",
	apiVersions:                 "ApiVersions",
	createTopics:                "CreateTopics",
	deleteTopics:                "DeleteTopics",
	deleteRecords:               "DeleteRecords",
	initProducerId:              "InitProducerId",
	offsetForLeaderEpoch:        "OffsetForLeaderEpoch",
	addPartitionsToTxn:          "AddPartitionsToTxn",
	addOffsetsToTxn:             "AddOffsetsToTxn",
	endTxn:                      "EndTxn",
	writeTxnMarkers:             "WriteTxnMarkers",
	txnOffsetCommit:             "TxnOffsetCommit",
	describeAcls:                "DescribeAcls",
	createAcls:                  "CreateAcls",
	deleteAcls:                  "DeleteAcls",
	describeConfigs:             "DescribeConfigs",
	alterConfigs:                "AlterConfigs",
	alterReplicaLogDirs:         "AlterReplicaLogDirs",
	describeLogDirs:             "DescribeLogDirs",
	saslAuthenticate:            "SaslAuthenticate",
	createPartitions:            "CreatePartitions",
	createDelegationToken:       "CreateDelegationToken",
	renewDelegationToken:        "RenewDelegationToken",
	expireDelegationToken:       "ExpireDelegationToken",
	describeDelegationToken:     "DescribeDelegationToken",
	deleteGroups:                "DeleteGroups",
	electLeaders:                "ElectLeaders",
	incrementalAlterConfigs:     "IncrementalAlfterConfigs",
	alterPartitionReassignments: "AlterPartitionReassignments",
	listPartitionReassignments:  "ListPartitionReassignments",
	offsetDelete:                "OffsetDelete",
}

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

func (h requestHeader) writeTo(wb *writeBuffer) {
	wb.writeInt32(h.Size)
	wb.writeInt16(h.ApiKey)
	wb.writeInt16(h.ApiVersion)
	wb.writeInt32(h.CorrelationID)
	wb.writeString(h.ClientID)
}

type request interface {
	size() int32
	writable
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
