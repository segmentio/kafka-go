package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	ktesting "github.com/segmentio/kafka-go/testing"
	"golang.org/x/net/nettest"
)

type timeout struct{}

func (*timeout) Error() string   { return "timeout" }
func (*timeout) Temporary() bool { return true }
func (*timeout) Timeout() bool   { return true }

// connPipe is an adapter that implements the net.Conn interface on top of
// two client kafka connections to pass the nettest.TestConn test suite.
type connPipe struct {
	rconn *Conn
	wconn *Conn
}

func (c *connPipe) Close() error {
	b := [1]byte{} // marker that the connection has been closed
	c.wconn.SetWriteDeadline(time.Time{})
	c.wconn.Write(b[:])
	c.wconn.Close()
	c.rconn.Close()
	return nil
}

func (c *connPipe) Read(b []byte) (int, error) {
	// See comments in Write.
	time.Sleep(time.Millisecond)
	if t := c.rconn.readDeadline(); !t.IsZero() {
		return 0, &timeout{}
	}
	n, err := c.rconn.Read(b)
	if n == 1 && b[0] == 0 {
		c.rconn.Close()
		n, err = 0, io.EOF
	}
	return n, err
}

func (c *connPipe) Write(b []byte) (int, error) {
	// The nettest/ConcurrentMethods test spawns a bunch of goroutines that do
	// random stuff on the connection, if a Read or Write was issued before a
	// deadline was set then it could cancel an inflight request to kafka,
	// resulting in the connection being closed.
	// To prevent this from happening we wait a little while to give the other
	// goroutines a chance to start and set the deadline.
	time.Sleep(time.Millisecond)

	// The nettest code only sets deadlines when it expects the write to time
	// out.  The broker connection is alive and able to accept data, so we need
	// to simulate the timeout in order to get the tests to pass.
	if t := c.wconn.writeDeadline(); !t.IsZero() {
		return 0, &timeout{}
	}

	return c.wconn.Write(b)
}

func (c *connPipe) LocalAddr() net.Addr {
	return c.rconn.LocalAddr()
}

func (c *connPipe) RemoteAddr() net.Addr {
	return c.wconn.LocalAddr()
}

func (c *connPipe) SetDeadline(t time.Time) error {
	c.rconn.SetDeadline(t)
	c.wconn.SetDeadline(t)
	return nil
}

func (c *connPipe) SetReadDeadline(t time.Time) error {
	return c.rconn.SetReadDeadline(t)
}

func (c *connPipe) SetWriteDeadline(t time.Time) error {
	return c.wconn.SetWriteDeadline(t)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func makeTopic() string {
	return fmt.Sprintf("kafka-go-%016x", rand.Int63())
}

func makeGroupID() string {
	return fmt.Sprintf("kafka-go-group-%016x", rand.Int63())
}

func makeTransactionalID() string {
	return fmt.Sprintf("kafka-go-transactional-id-%016x", rand.Int63())
}

func TestConn(t *testing.T) {
	tests := []struct {
		scenario   string
		function   func(*testing.T, *Conn)
		minVersion string
	}{
		{
			scenario: "close right away",
			function: testConnClose,
		},

		{
			scenario: "ensure the initial offset of a connection is the first offset",
			function: testConnFirstOffset,
		},

		{
			scenario: "write a single message to kafka should succeed",
			function: testConnWrite,
		},

		{
			scenario: "writing a message to a closed kafka connection should fail",
			function: testConnCloseAndWrite,
		},

		{
			scenario: "ensure the connection can seek to the first offset",
			function: testConnSeekFirstOffset,
		},

		{
			scenario: "ensure the connection can seek to the last offset",
			function: testConnSeekLastOffset,
		},

		{
			scenario: "ensure the connection can seek relative to the current offset",
			function: testConnSeekCurrentOffset,
		},

		{
			scenario: "ensure the connection can seek to a random offset",
			function: testConnSeekRandomOffset,
		},

		{
			scenario: "unchecked seeks allow the connection to be positioned outside the boundaries of the partition",
			function: testConnSeekDontCheck,
		},

		{
			scenario: "writing and reading messages sequentially should preserve the order",
			function: testConnWriteReadSequentially,
		},

		{
			scenario: "writing a batch of messages and reading it sequentially should preserve the order",
			function: testConnWriteBatchReadSequentially,
		},

		{
			scenario: "writing and reading messages concurrently should preserve the order",
			function: testConnWriteReadConcurrently,
		},

		{
			scenario: "reading messages with a buffer that is too short should return io.ErrShortBuffer and maintain the connection open",
			function: testConnReadShortBuffer,
		},

		{
			scenario: "reading messages from an empty partition should timeout after reaching the deadline",
			function: testConnReadEmptyWithDeadline,
		},

		{
			scenario: "write batch of messages and read the highest offset (watermark)",
			function: testConnReadWatermarkFromBatch,
		},

		{
			scenario:   "read a batch with no explicit min or max bytes",
			function:   testConnReadBatchWithNoMinMaxBytes,
			minVersion: "0.11.0",
		},

		{
			scenario: "read a batch using explicit max wait time",
			function: testConnReadBatchWithMaxWait,
		},

		{
			scenario: "find the group coordinator",
			function: testConnFindCoordinator,
		},

		{
			scenario: "test join group with an invalid groupID",
			function: testConnJoinGroupInvalidGroupID,
		},

		{
			scenario: "test join group with an invalid sessionTimeout",
			function: testConnJoinGroupInvalidSessionTimeout,
		},

		{
			scenario: "test join group with an invalid refreshTimeout",
			function: testConnJoinGroupInvalidRefreshTimeout,
		},

		{
			scenario: "test heartbeat once group has been created",
			function: testConnHeartbeatErr,
		},

		{
			scenario: "test leave group returns error when called outside group",
			function: testConnLeaveGroupErr,
		},

		{
			scenario: "test sync group with bad memberID",
			function: testConnSyncGroupErr,
		},

		{
			scenario:   "test list groups",
			function:   testConnListGroupsReturnsGroups,
			minVersion: "0.11.0",
		},

		{
			scenario: "test fetch and commit offset",
			function: testConnFetchAndCommitOffsets,
		},

		{
			scenario: "test delete topics",
			function: testDeleteTopics,
		},

		{
			scenario: "test delete topics with an invalid topic",
			function: testDeleteTopicsInvalidTopic,
		},

		{
			scenario: "test retrieve controller",
			function: testController,
		},

		{
			scenario: "test list brokers",
			function: testBrokers,
		},

		{
			scenario: "the connection advertises the broker that it is connected to",
			function: testConnBroker,
		},
	}

	const (
		tcp   = "tcp"
		kafka = "localhost:9092"
	)

	for _, test := range tests {
		if !ktesting.KafkaIsAtLeast(test.minVersion) {
			t.Log("skipping " + test.scenario + " because broker is not at least version " + test.minVersion)
			continue
		}

		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			topic := makeTopic()

			conn, err := (&Dialer{
				Resolver: &net.Resolver{},
			}).DialLeader(ctx, tcp, kafka, topic, 0)
			if err != nil {
				t.Fatal("failed to open a new kafka connection:", err)
			}
			defer conn.Close()
			testFunc(t, conn)
		})
	}

	t.Run("nettest", func(t *testing.T) {
		// Need ability to skip nettest on newer Kafka versions to avoid these kinds of errors:
		//  --- FAIL: TestConn/nettest (17.56s)
		//    --- FAIL: TestConn/nettest/PingPong (7.40s)
		//      conntest.go:112: unexpected Read error: [7] Request Timed Out: the request exceeded the user-specified time limit in the request
		//      conntest.go:118: mismatching value: got 77, want 78
		//      conntest.go:118: mismatching value: got 78, want 79
		// ...
		//
		// TODO: Figure out why these are happening and fix them (they don't appear to be new).
		if _, ok := os.LookupEnv("KAFKA_SKIP_NETTEST"); ok {
			t.Log("skipping nettest because KAFKA_SKIP_NETTEST is set")
			t.Skip()
		}

		t.Parallel()

		nettest.TestConn(t, func() (c1 net.Conn, c2 net.Conn, stop func(), err error) {
			topic1 := makeTopic()
			topic2 := makeTopic()
			var t1Reader *Conn
			var t2Reader *Conn
			var t1Writer *Conn
			var t2Writer *Conn
			dialer := &Dialer{}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if t1Reader, err = dialer.DialLeader(ctx, tcp, kafka, topic1, 0); err != nil {
				return
			}
			if t2Reader, err = dialer.DialLeader(ctx, tcp, kafka, topic2, 0); err != nil {
				return
			}
			if t1Writer, err = dialer.DialLeader(ctx, tcp, kafka, topic1, 0); err != nil {
				return
			}
			if t2Writer, err = dialer.DialLeader(ctx, tcp, kafka, topic2, 0); err != nil {
				return
			}

			stop = func() {
				t1Reader.Close()
				t1Writer.Close()
				t2Reader.Close()
				t2Writer.Close()
			}
			c1 = &connPipe{rconn: t1Reader, wconn: t2Writer}
			c2 = &connPipe{rconn: t2Reader, wconn: t1Writer}
			return
		})
	})
}

func TestWriteMulti(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const (
		tcp   = "tcp"
		kafka = "localhost:9092"
	)

	conn, err := (&Dialer{
		Resolver: &net.Resolver{},
	}).DialContext(ctx, tcp, kafka)
	if err != nil {
		t.Fatal("failed to open a new kafka connection:", err)
	}
	defer conn.Close()

	topic1 := makeTopic()
	topic2 := makeTopic()

	err = conn.CreateTopics(
		TopicConfig{
			Topic:             topic1,
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
		TopicConfig{
			Topic:             topic2,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	)
	if err != nil {
		t.Fatal("failed creating topics: ", err)
	}

	conn.SetWriteDeadline(time.Now().Add(time.Minute))

	err = conn.WriteMulti(nil,
		MultiProduceRequest{
			Topic: topic1,
			Partitions: []MultiProducePartition{
				{
					Partition: 0,
					Messages: []Message{
						{
							Value: []byte("0.0"),
						},
						{
							Value: []byte("0.1"),
						},
						{
							Value: []byte("0.2"),
						},
					},
				},
				{
					Partition: 1,
					Messages: []Message{
						{
							Value: []byte("1.0"),
						},
						{
							Value: []byte("1.1"),
						},
						{
							Value: []byte("1.2"),
						},
					},
				},
			},
		},
		MultiProduceRequest{
			Topic: topic2,
			Partitions: []MultiProducePartition{
				{
					Partition: 0,
					Messages: []Message{
						{
							Value: []byte("a.0"),
						},
						{
							Value: []byte("b.1"),
						},
						{
							Value: []byte("c.2"),
						},
					},
				},
			},
		},
	)
	if err != nil {
		t.Fatal("failed writing: ", err)
	}

	err = conn.WriteMulti(nil,
		MultiProduceRequest{
			Topic: topic1,
			Partitions: []MultiProducePartition{
				{
					Partition: 0,
					Messages: []Message{
						{
							Value: []byte("0.0"),
						},
						{
							Value: []byte("0.1"),
						},
						{
							Value: []byte("0.2"),
						},
					},
				},
				{
					Partition: 5,
					Messages: []Message{
						{
							Value: []byte("1.0"),
						},
						{
							Value: []byte("1.1"),
						},
						{
							Value: []byte("1.2"),
						},
					},
				},
			},
		},
		MultiProduceRequest{
			Topic: "does-not-exist",
			Partitions: []MultiProducePartition{
				{
					Partition: 0,
					Messages: []Message{
						{
							Value: []byte("a.0"),
						},
						{
							Value: []byte("b.1"),
						},
						{
							Value: []byte("c.2"),
						},
					},
				},
			},
		},
	)
	if err == nil {
		t.Fatal("should have failed with error")
	}
	var mpErr MultiProduceError
	if !errors.As(err, &mpErr) {
		t.Fatal("should have been a multi-produce error")
	}
	if len(mpErr) != 2 {
		t.Fatal("expected two errors")
	}

	// ordering is not guaranteed by the broker, so we need to sort
	sort.Slice(mpErr, func(i, j int) bool {
		if mpErr[i].Topic < mpErr[j].Topic {
			return true
		}
		if mpErr[i].Topic > mpErr[j].Topic {
			return false
		}
		return mpErr[i].Partition < mpErr[j].Partition
	})

	expected := MultiProduceError{
		{
			Topic:     "does-not-exist",
			Partition: 0,
			Error:     UnknownTopicOrPartition,
		},
		{
			Topic:     topic1,
			Partition: 5,
			Error:     UnknownTopicOrPartition,
		},
	}
	if !reflect.DeepEqual(expected, mpErr) {
		t.Fatalf("expected %v but got %v", expected, mpErr)
	}
}

func testConnClose(t *testing.T, conn *Conn) {
	if err := conn.Close(); err != nil {
		t.Error(err)
	}
}

func testConnFirstOffset(t *testing.T, conn *Conn) {
	offset, whence := conn.Offset()

	if offset != 0 && whence != 0 {
		t.Error("bad first offset:", offset, whence)
	}
}

func testConnWrite(t *testing.T, conn *Conn) {
	b := []byte("Hello World!")
	n, err := conn.Write(b)
	if err != nil {
		t.Error(err)
	}

	if n != len(b) {
		t.Error("bad length returned by (*Conn).Write:", n)
	}
}

func testConnCloseAndWrite(t *testing.T, conn *Conn) {
	conn.Close()

	_, err := conn.Write([]byte("Hello World!"))

	// expect a network error
	var netOpError *net.OpError
	if !errors.As(err, &netOpError) {
		t.Error(err)
	}
}

func testConnSeekFirstOffset(t *testing.T, conn *Conn) {
	for i := 0; i != 10; i++ {
		if _, err := conn.Write([]byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	offset, err := conn.Seek(0, SeekStart)
	if err != nil {
		t.Error(err)
	}

	if offset != 0 {
		t.Error("bad offset:", offset)
	}
}

func testConnSeekLastOffset(t *testing.T, conn *Conn) {
	for i := 0; i != 10; i++ {
		if _, err := conn.Write([]byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	offset, err := conn.Seek(0, SeekEnd)
	if err != nil {
		t.Error(err)
	}

	if offset != 10 {
		t.Error("bad offset:", offset)
	}
}

func testConnSeekCurrentOffset(t *testing.T, conn *Conn) {
	for i := 0; i != 10; i++ {
		if _, err := conn.Write([]byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	offset, err := conn.Seek(5, SeekStart)
	if err != nil {
		t.Error(err)
	}

	if offset != 5 {
		t.Error("bad offset:", offset)
	}

	offset, err = conn.Seek(-2, SeekCurrent)
	if err != nil {
		t.Error(err)
	}

	if offset != 3 {
		t.Error("bad offset:", offset)
	}
}

func testConnSeekRandomOffset(t *testing.T, conn *Conn) {
	for i := 0; i != 10; i++ {
		if _, err := conn.Write([]byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	offset, err := conn.Seek(3, SeekAbsolute)
	if err != nil {
		t.Error(err)
	}

	if offset != 3 {
		t.Error("bad offset:", offset)
	}
}

func testConnSeekDontCheck(t *testing.T, conn *Conn) {
	for i := 0; i != 10; i++ {
		if _, err := conn.Write([]byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	offset, err := conn.Seek(42, SeekAbsolute|SeekDontCheck)
	if err != nil {
		t.Error(err)
	}

	if offset != 42 {
		t.Error("bad offset:", offset)
	}

	if _, err := conn.ReadMessage(1024); !errors.Is(err, OffsetOutOfRange) {
		t.Error("unexpected error:", err)
	}
}

func testConnWriteReadSequentially(t *testing.T, conn *Conn) {
	for i := 0; i != 10; i++ {
		if _, err := conn.Write([]byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	b := make([]byte, 128)

	for i := 0; i != 10; i++ {
		n, err := conn.Read(b)
		if err != nil {
			t.Error(err)
			continue
		}
		s := string(b[:n])
		if v, err := strconv.Atoi(s); err != nil {
			t.Error(err)
		} else if v != i {
			t.Errorf("bad message read at offset %d: %s", i, s)
		}
	}
}

func testConnWriteBatchReadSequentially(t *testing.T, conn *Conn) {
	msgs := makeTestSequence(10)

	if _, err := conn.WriteMessages(msgs...); err != nil {
		t.Fatal(err)
	}

	for i := 0; i != 10; i++ {
		msg, err := conn.ReadMessage(128)
		if err != nil {
			t.Error(err)
			continue
		}
		if !bytes.Equal(msg.Key, msgs[i].Key) {
			t.Errorf("bad message key at offset %d: %q != %q", i, msg.Key, msgs[i].Key)
		}
		if !bytes.Equal(msg.Value, msgs[i].Value) {
			t.Errorf("bad message value at offset %d: %q != %q", i, msg.Value, msgs[i].Value)
		}
		if !msg.Time.Equal(msgs[i].Time) {
			t.Errorf("bad message time at offset %d: %s != %s", i, msg.Time, msgs[i].Time)
		}
	}
}

func testConnReadWatermarkFromBatch(t *testing.T, conn *Conn) {
	if _, err := conn.WriteMessages(makeTestSequence(10)...); err != nil {
		t.Fatal(err)
	}

	const minBytes = 1
	const maxBytes = 10e6 // 10 MB

	value := make([]byte, 10e3) // 10 KB

	batch := conn.ReadBatch(minBytes, maxBytes)

	for i := 0; i < 10; i++ {
		_, err := batch.Read(value)
		if err != nil {
			if err = batch.Close(); err != nil {
				t.Fatalf("error trying to read batch message: %s", err)
			}
		}

		if batch.HighWaterMark() != 10 {
			t.Fatal("expected highest offset (watermark) to be 10")
		}
	}

	batch.Close()
}

func testConnReadBatchWithNoMinMaxBytes(t *testing.T, conn *Conn) {
	if _, err := conn.WriteMessages(makeTestSequence(10)...); err != nil {
		t.Fatal(err)
	}

	value := make([]byte, 10e3) // 10 KB

	batch := conn.ReadBatchWith(ReadBatchConfig{})

	for i := 0; i < 10; i++ {
		_, err := batch.Read(value)
		if err != nil {
			if err = batch.Close(); err != nil {
				t.Fatalf("error trying to read batch message: %s", err)
			}
		}

		if batch.HighWaterMark() != 10 {
			t.Fatal("expected highest offset (watermark) to be 10")
		}
	}

	if err := batch.Close(); err != nil {
		t.Fatalf("error trying to close batch: %s", err)
	}

	if err := batch.Err(); err != nil {
		t.Fatalf("broken batch: %s", err)
	}
}

func testConnReadBatchWithMaxWait(t *testing.T, conn *Conn) {
	if _, err := conn.WriteMessages(makeTestSequence(10)...); err != nil {
		t.Fatal(err)
	}

	const maxBytes = 10e6 // 10 MB

	value := make([]byte, 10e3) // 10 KB

	cfg := ReadBatchConfig{
		MinBytes: maxBytes, // use max for both so that we hit max wait time
		MaxBytes: maxBytes,
		MaxWait:  500 * time.Millisecond,
	}

	// set aa read deadline so the batch will succeed.
	conn.SetDeadline(time.Now().Add(time.Second))
	batch := conn.ReadBatchWith(cfg)

	for i := 0; i < 10; i++ {
		_, err := batch.Read(value)
		if err != nil {
			if err = batch.Close(); err != nil {
				t.Fatalf("error trying to read batch message: %s", err)
			}
		}

		if batch.HighWaterMark() != 10 {
			t.Fatal("expected highest offset (watermark) to be 10")
		}
	}

	batch.Close()

	// reset the offset and  ensure that the conn deadline takes precedence over
	// the max wait
	conn.Seek(0, SeekAbsolute)
	conn.SetDeadline(time.Now().Add(50 * time.Millisecond))
	batch = conn.ReadBatchWith(cfg)
	var netErr net.Error
	if err := batch.Err(); err == nil {
		t.Fatal("should have timed out, but got no error")
	} else if errors.As(err, &netErr) {
		if !netErr.Timeout() {
			t.Fatalf("should have timed out, but got: %v", err)
		}
	}
}

func waitForCoordinator(t *testing.T, conn *Conn, groupID string) {
	// ensure that kafka has allocated a group coordinator.  oddly, issue doesn't
	// appear to happen if the kafka been running for a while.
	const maxAttempts = 20
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		_, err := conn.findCoordinator(findCoordinatorRequestV0{
			CoordinatorKey: groupID,
		})
		if err != nil {
			if errors.Is(err, GroupCoordinatorNotAvailable) {
				time.Sleep(250 * time.Millisecond)
				continue
			} else {
				t.Fatalf("unable to find coordinator for group: %v", err)
			}
		} else {
			return
		}
	}

	t.Fatalf("unable to connect to coordinator after %v attempts", maxAttempts)
}

func createGroup(t *testing.T, conn *Conn, groupID string) (generationID int32, memberID string, stop func()) {
	waitForCoordinator(t, conn, groupID)

	join := func() (joinGroup joinGroupResponseV1) {
		var err error
		for attempt := 0; attempt < 10; attempt++ {
			joinGroup, err = conn.joinGroup(joinGroupRequestV1{
				GroupID:          groupID,
				SessionTimeout:   int32(time.Minute / time.Millisecond),
				RebalanceTimeout: int32(time.Second / time.Millisecond),
				ProtocolType:     "roundrobin",
				GroupProtocols: []joinGroupRequestGroupProtocolV1{
					{
						ProtocolName:     "roundrobin",
						ProtocolMetadata: []byte("blah"),
					},
				},
			})
			if err != nil {
				if errors.Is(err, NotCoordinatorForGroup) {
					time.Sleep(250 * time.Millisecond)
					continue
				} else {
					t.Fatalf("bad joinGroup: %s", err)
				}
			} else {
				return
			}
		}

		return
	}

	// join the group
	joinGroup := join()

	// sync the group
	_, err := conn.syncGroup(syncGroupRequestV0{
		GroupID:      groupID,
		GenerationID: joinGroup.GenerationID,
		MemberID:     joinGroup.MemberID,
		GroupAssignments: []syncGroupRequestGroupAssignmentV0{
			{
				MemberID:          joinGroup.MemberID,
				MemberAssignments: []byte("blah"),
			},
		},
	})
	if err != nil {
		t.Fatalf("bad syncGroup: %s", err)
	}

	generationID = joinGroup.GenerationID
	memberID = joinGroup.MemberID
	stop = func() {
		conn.leaveGroup(leaveGroupRequestV0{
			GroupID:  groupID,
			MemberID: joinGroup.MemberID,
		})
	}

	return
}

func testConnFindCoordinator(t *testing.T, conn *Conn) {
	groupID := makeGroupID()

	for attempt := 0; attempt < 10; attempt++ {
		if attempt != 0 {
			time.Sleep(time.Millisecond * 50)
		}
		response, err := conn.findCoordinator(findCoordinatorRequestV0{CoordinatorKey: groupID})
		if err != nil {
			if errors.Is(err, GroupCoordinatorNotAvailable) {
				continue
			}

			t.Fatalf("bad findCoordinator: %s", err)
		}

		if response.Coordinator.NodeID == 0 {
			t.Errorf("bad NodeID")
		}
		if response.Coordinator.Host == "" {
			t.Errorf("bad Host")
		}
		if response.Coordinator.Port == 0 {
			t.Errorf("bad Port")
		}
		return
	}
}

func testConnJoinGroupInvalidGroupID(t *testing.T, conn *Conn) {
	_, err := conn.joinGroup(joinGroupRequestV1{})
	if !errors.Is(err, InvalidGroupId) && !errors.Is(err, NotCoordinatorForGroup) {
		t.Fatalf("expected %v or %v; got %v", InvalidGroupId, NotCoordinatorForGroup, err)
	}
}

func testConnJoinGroupInvalidSessionTimeout(t *testing.T, conn *Conn) {
	groupID := makeGroupID()
	waitForCoordinator(t, conn, groupID)

	_, err := conn.joinGroup(joinGroupRequestV1{
		GroupID: groupID,
	})
	if !errors.Is(err, InvalidSessionTimeout) && !errors.Is(err, NotCoordinatorForGroup) {
		t.Fatalf("expected %v or %v; got %v", InvalidSessionTimeout, NotCoordinatorForGroup, err)
	}
}

func testConnJoinGroupInvalidRefreshTimeout(t *testing.T, conn *Conn) {
	groupID := makeGroupID()
	waitForCoordinator(t, conn, groupID)

	_, err := conn.joinGroup(joinGroupRequestV1{
		GroupID:        groupID,
		SessionTimeout: int32(3 * time.Second / time.Millisecond),
	})
	if !errors.Is(err, InvalidSessionTimeout) && !errors.Is(err, NotCoordinatorForGroup) {
		t.Fatalf("expected %v or %v; got %v", InvalidSessionTimeout, NotCoordinatorForGroup, err)
	}
}

func testConnHeartbeatErr(t *testing.T, conn *Conn) {
	groupID := makeGroupID()
	createGroup(t, conn, groupID)

	_, err := conn.syncGroup(syncGroupRequestV0{
		GroupID: groupID,
	})
	if !errors.Is(err, UnknownMemberId) && !errors.Is(err, NotCoordinatorForGroup) {
		t.Fatalf("expected %v or %v; got %v", UnknownMemberId, NotCoordinatorForGroup, err)
	}
}

func testConnLeaveGroupErr(t *testing.T, conn *Conn) {
	groupID := makeGroupID()
	waitForCoordinator(t, conn, groupID)

	_, err := conn.leaveGroup(leaveGroupRequestV0{
		GroupID: groupID,
	})
	if !errors.Is(err, UnknownMemberId) && !errors.Is(err, NotCoordinatorForGroup) {
		t.Fatalf("expected %v or %v; got %v", UnknownMemberId, NotCoordinatorForGroup, err)
	}
}

func testConnSyncGroupErr(t *testing.T, conn *Conn) {
	groupID := makeGroupID()
	waitForCoordinator(t, conn, groupID)

	_, err := conn.syncGroup(syncGroupRequestV0{
		GroupID: groupID,
	})
	if !errors.Is(err, UnknownMemberId) && !errors.Is(err, NotCoordinatorForGroup) {
		t.Fatalf("expected %v or %v; got %v", UnknownMemberId, NotCoordinatorForGroup, err)
	}
}

func testConnListGroupsReturnsGroups(t *testing.T, conn *Conn) {
	group1 := makeGroupID()
	_, _, stop1 := createGroup(t, conn, group1)
	defer stop1()

	group2 := makeGroupID()
	_, _, stop2 := createGroup(t, conn, group2)
	defer stop2()

	out, err := conn.listGroups(listGroupsRequestV1{})
	if err != nil {
		t.Fatalf("bad err: %v", err)
	}

	containsGroup := func(groupID string) bool {
		for _, group := range out.Groups {
			if group.GroupID == groupID {
				return true
			}
		}
		return false
	}

	if !containsGroup(group1) {
		t.Errorf("expected groups to contain group1")
	}

	if !containsGroup(group2) {
		t.Errorf("expected groups to contain group2")
	}
}

func testConnFetchAndCommitOffsets(t *testing.T, conn *Conn) {
	const N = 10
	if _, err := conn.WriteMessages(makeTestSequence(N)...); err != nil {
		t.Fatal(err)
	}

	groupID := makeGroupID()
	generationID, memberID, stop := createGroup(t, conn, groupID)
	defer stop()

	request := offsetFetchRequestV1{
		GroupID: groupID,
		Topics: []offsetFetchRequestV1Topic{
			{
				Topic:      conn.topic,
				Partitions: []int32{0},
			},
		},
	}
	fetch, err := conn.offsetFetch(request)
	if err != nil {
		t.Fatalf("bad err: %v", err)
	}

	if v := len(fetch.Responses); v != 1 {
		t.Fatalf("expected 1 Response; got %v", v)
	}

	if v := len(fetch.Responses[0].PartitionResponses); v != 1 {
		t.Fatalf("expected 1 PartitionResponses; got %v", v)
	}

	if offset := fetch.Responses[0].PartitionResponses[0].Offset; offset != -1 {
		t.Fatalf("expected initial offset of -1; got %v", offset)
	}

	committedOffset := int64(N - 1)
	_, err = conn.offsetCommit(offsetCommitRequestV2{
		GroupID:       groupID,
		GenerationID:  generationID,
		MemberID:      memberID,
		RetentionTime: int64(time.Hour / time.Millisecond),
		Topics: []offsetCommitRequestV2Topic{
			{
				Topic: conn.topic,
				Partitions: []offsetCommitRequestV2Partition{
					{
						Partition: 0,
						Offset:    committedOffset,
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("bad error: %v", err)
	}

	fetch, err = conn.offsetFetch(request)
	if err != nil {
		t.Fatalf("bad error: %v", err)
	}

	fetchedOffset := fetch.Responses[0].PartitionResponses[0].Offset
	if committedOffset != fetchedOffset {
		t.Fatalf("bad offset.  expected %v; got %v", committedOffset, fetchedOffset)
	}
}

func testConnWriteReadConcurrently(t *testing.T, conn *Conn) {
	const N = 1000
	msgs := make([]string, N)
	done := make(chan struct{})
	written := make(chan struct{}, N/10)

	for i := 0; i != N; i++ {
		msgs[i] = strconv.Itoa(i)
	}

	go func() {
		defer close(done)
		for _, msg := range msgs {
			if _, err := conn.Write([]byte(msg)); err != nil {
				t.Error(err)
			}
			written <- struct{}{}
		}
	}()

	b := make([]byte, 128)

	for i := 0; i != N; i++ {
		// wait until at least one message has been written.  the reason for
		// this synchronization is that we aren't using deadlines.  as such, if
		// the read happens before a message is available, it will cause a
		// deadlock because the read request will never hit the one byte minimum
		// in order to return and release the lock on the conn.  by ensuring
		// that there's at least one message produced, we don't hit that
		// condition.
		<-written
		n, err := conn.Read(b)
		if err != nil {
			t.Error(err)
		}
		if s := string(b[:n]); s != strconv.Itoa(i) {
			t.Errorf("bad message read at offset %d: %s", i, s)
		}
	}

	<-done
}

func testConnReadShortBuffer(t *testing.T, conn *Conn) {
	if _, err := conn.Write([]byte("Hello World!")); err != nil {
		t.Fatal(err)
	}

	b := make([]byte, 4)

	for i := 0; i != 10; i++ {
		b[0] = 0
		b[1] = 0
		b[2] = 0
		b[3] = 0

		n, err := conn.Read(b)
		if !errors.Is(err, io.ErrShortBuffer) {
			t.Error("bad error:", i, err)
		}
		if n != 4 {
			t.Error("bad byte count:", i, n)
		}
		if s := string(b); s != "Hell" {
			t.Error("bad content:", i, s)
		}
	}
}

func testConnReadEmptyWithDeadline(t *testing.T, conn *Conn) {
	b := make([]byte, 100)

	start := time.Now()
	deadline := start.Add(time.Second)

	conn.SetReadDeadline(deadline)
	n, err := conn.Read(b)

	if n != 0 {
		t.Error("bad byte count:", n)
	}

	if !isTimeout(err) {
		t.Error("expected timeout error but got", err)
	}
}

func testDeleteTopics(t *testing.T, conn *Conn) {
	topic1 := makeTopic()
	topic2 := makeTopic()
	err := conn.CreateTopics(
		TopicConfig{
			Topic:             topic1,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
		TopicConfig{
			Topic:             topic2,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	)
	if err != nil {
		t.Fatalf("bad CreateTopics: %v", err)
	}
	conn.SetDeadline(time.Now().Add(time.Second))
	err = conn.DeleteTopics(topic1, topic2)
	if err != nil {
		t.Fatalf("bad DeleteTopics: %v", err)
	}
	partitions, err := conn.ReadPartitions(topic1, topic2)
	if err != nil {
		t.Fatalf("bad ReadPartitions: %v", err)
	}
	if len(partitions) != 0 {
		t.Fatal("exepected partitions to be empty ")
	}
}

func testDeleteTopicsInvalidTopic(t *testing.T, conn *Conn) {
	topic := makeTopic()
	err := conn.CreateTopics(
		TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	)
	if err != nil {
		t.Fatalf("bad CreateTopics: %v", err)
	}
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	err = conn.DeleteTopics("invalid-topic", topic)
	if !errors.Is(err, UnknownTopicOrPartition) {
		t.Fatalf("expected UnknownTopicOrPartition error, but got %v", err)
	}
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		t.Fatalf("bad ReadPartitions: %v", err)
	}
	if len(partitions) != 0 {
		t.Fatal("expected partitions to be empty")
	}
}

func testController(t *testing.T, conn *Conn) {
	b, err := conn.Controller()
	if err != nil {
		t.Error(err)
	}

	if b.Host != "localhost" {
		t.Errorf("expected localhost received %s", b.Host)
	}
	if b.Port != 9092 {
		t.Errorf("expected 9092 received %d", b.Port)
	}
	if b.ID != 1 {
		t.Errorf("expected 1 received %d", b.ID)
	}
	if b.Rack != "" {
		t.Errorf("expected empty string for rack received %s", b.Rack)
	}
}

func testBrokers(t *testing.T, conn *Conn) {
	brokers, err := conn.Brokers()
	if err != nil {
		t.Error(err)
	}

	if len(brokers) != 1 {
		t.Errorf("expected 1 broker in %+v", brokers)
	}

	if brokers[0].ID != 1 {
		t.Errorf("expected ID 1 received %d", brokers[0].ID)
	}
}

func testConnBroker(t *testing.T, conn *Conn) {
	broker := conn.Broker()
	// Depending on the environment the test is being run, IPv4 or IPv6 may be used.
	if broker.Host != "::1" && broker.Host != "127.0.0.1" {
		t.Errorf("invalid broker address: %q", broker.Host)
	}
	if broker.Port != 9092 {
		t.Errorf("invalid broker port: %d", broker.Port)
	}
	if broker.ID != 1 {
		t.Errorf("invalid broker id: %d", broker.ID)
	}
	if broker.Rack != "" {
		t.Errorf("invalid broker rack: %q", broker.Rack)
	}
}

func TestReadPartitionsNoTopic(t *testing.T) {
	conn, err := Dial("tcp", "127.0.0.1:9092")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	parts, err := conn.ReadPartitions()
	if err != nil {
		t.Error(err)
	}

	if len(parts) == 0 {
		t.Errorf("no partitions were returned")
	}
}

func TestUnsupportedSASLMechanism(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := (&Dialer{
		Resolver: &net.Resolver{},
	}).DialContext(ctx, "tcp", "127.0.0.1:9093")
	if err != nil {
		t.Fatal("failed to open a new kafka connection:", err)
	}
	defer conn.Close()

	if err := conn.saslHandshake("FOO"); !errors.Is(err, UnsupportedSASLMechanism) {
		t.Errorf("Expected UnsupportedSASLMechanism but got %v", err)
	}
}

const benchmarkMessageCount = 100

func BenchmarkConn(b *testing.B) {
	benchmarks := []struct {
		scenario string
		function func(*testing.B, *Conn, []byte)
	}{
		{
			scenario: "Seek",
			function: benchmarkConnSeek,
		},

		{
			scenario: "Read",
			function: benchmarkConnRead,
		},

		{
			scenario: "ReadBatch",
			function: benchmarkConnReadBatch,
		},

		{
			scenario: "ReadOffsets",
			function: benchmarkConnReadOffsets,
		},

		{
			scenario: "Write",
			function: benchmarkConnWrite,
		},
	}

	topic := makeTopic()
	value := make([]byte, 10e3) // 10 KB
	msgs := make([]Message, benchmarkMessageCount)

	for i := range msgs {
		msgs[i].Value = value
	}

	conn, _ := DialLeader(context.Background(), "tcp", "localhost:9092", topic, 0)
	defer conn.Close()

	if _, err := conn.WriteMessages(msgs...); err != nil {
		b.Fatal(err)
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.scenario, func(b *testing.B) {
			if _, err := conn.Seek(0, SeekStart); err != nil {
				b.Error(err)
				return
			}
			benchmark.function(b, conn, value)
		})
	}
}

func benchmarkConnSeek(b *testing.B, conn *Conn, _ []byte) {
	for i := 0; i != b.N; i++ {
		if _, err := conn.Seek(int64(i%benchmarkMessageCount), SeekAbsolute); err != nil {
			b.Error(err)
			return
		}
	}
}

func benchmarkConnRead(b *testing.B, conn *Conn, a []byte) {
	n := 0
	i := 0

	for i != b.N {
		if (i % benchmarkMessageCount) == 0 {
			if _, err := conn.Seek(0, SeekStart); err != nil {
				b.Error(err)
				return
			}
		}

		c, err := conn.Read(a)
		if err != nil {
			b.Error(err)
			return
		}

		n += c
		i++
	}

	b.SetBytes(int64(n / i))
}

func benchmarkConnReadBatch(b *testing.B, conn *Conn, a []byte) {
	const minBytes = 1
	const maxBytes = 10e6 // 10 MB

	batch := conn.ReadBatch(minBytes, maxBytes)
	i := 0
	n := 0

	for i != b.N {
		c, err := batch.Read(a)
		if err != nil {
			if err = batch.Close(); err != nil {
				b.Error(err)
				return
			}
			if _, err = conn.Seek(0, SeekStart); err != nil {
				b.Error(err)
				return
			}
			batch = conn.ReadBatch(minBytes, maxBytes)
		}
		n += c
		i++
	}

	batch.Close()
	b.SetBytes(int64(n / i))
}

func benchmarkConnReadOffsets(b *testing.B, conn *Conn, _ []byte) {
	for i := 0; i != b.N; i++ {
		_, _, err := conn.ReadOffsets()
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func benchmarkConnWrite(b *testing.B, conn *Conn, _ []byte) {
	a := make([]byte, 10e3) // 10 KB
	n := 0
	i := 0

	for i != b.N {
		c, err := conn.Write(a)
		if err != nil {
			b.Error(err)
			return
		}
		n += c
		i++
	}

	b.SetBytes(int64(n / i))
}

func TestEmptyToNullableReturnsNil(t *testing.T) {
	if emptyToNullable("") != nil {
		t.Error("Empty string is not converted to nil")
	}
}

func TestEmptyToNullableLeavesStringsIntact(t *testing.T) {
	const s = "abc"
	r := emptyToNullable(s)
	if *r != s {
		t.Error("Non empty string is not equal to the original string")
	}
}

func TestMakeBrokersAllPresent(t *testing.T) {
	brokers := make(map[int32]Broker)
	brokers[1] = Broker{ID: 1, Host: "203.0.113.101", Port: 9092}
	brokers[2] = Broker{ID: 1, Host: "203.0.113.102", Port: 9092}
	brokers[3] = Broker{ID: 1, Host: "203.0.113.103", Port: 9092}

	b := makeBrokers(brokers, 1, 2, 3)
	if len(b) != 3 {
		t.Errorf("Expected 3 brokers, got %d", len(b))
	}
	for _, i := range []int32{1, 2, 3} {
		if b[i-1] != brokers[i] {
			t.Errorf("Expected broker %d at index %d, got %d", i, i-1, b[i].ID)
		}
	}
}

func TestMakeBrokersOneMissing(t *testing.T) {
	brokers := make(map[int32]Broker)
	brokers[1] = Broker{ID: 1, Host: "203.0.113.101", Port: 9092}
	brokers[3] = Broker{ID: 3, Host: "203.0.113.103", Port: 9092}

	b := makeBrokers(brokers, 1, 2, 3)
	if len(b) != 3 {
		t.Errorf("Expected 3 brokers, got %d", len(b))
	}
	if b[0] != brokers[1] {
		t.Errorf("Expected broker 1 at index 0, got %d", b[0].ID)
	}
	if b[1] != (Broker{ID: 2}) {
		t.Errorf("Expected broker 2 at index 1, got %d", b[1].ID)
	}
	if b[2] != brokers[3] {
		t.Errorf("Expected broker 3 at index 1, got %d", b[2].ID)
	}
}
