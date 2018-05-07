package kafka

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

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
	if t := c.rconn.readDeadline(); !t.IsZero() && t.Sub(time.Now()) <= (10*time.Millisecond) {
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

	// Some tests set very short deadlines which end up aborting requests and
	// closing the connection. To prevent this from happening we check how far
	// the deadline is and if it's too close we timeout.
	if t := c.wconn.writeDeadline(); !t.IsZero() && t.Sub(time.Now()) <= (10*time.Millisecond) {
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

func TestConn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T, *Conn)
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
			scenario: "describe groups retrieves all groups when no groupID specified",
			function: testConnDescribeGroupRetrievesAllGroups,
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
			scenario: "test list groups",
			function: testConnListGroupsReturnsGroups,
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
	}

	const (
		tcp   = "tcp"
		kafka = "localhost:9092"
	)

	for _, test := range tests {
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
		t.Parallel()

		nettest.TestConn(t, func() (c1 net.Conn, c2 net.Conn, stop func(), err error) {
			var topic1 = makeTopic()
			var topic2 = makeTopic()
			var t1Reader *Conn
			var t2Reader *Conn
			var t1Writer *Conn
			var t2Writer *Conn
			var dialer = &Dialer{}

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

	switch _, err := conn.Write([]byte("Hello World!")); err.(type) {
	case *net.OpError:
	default:
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
	if _, err := conn.WriteMessages(makeTestSequence(10)...); err != nil {
		t.Fatal(err)
	}

	for i := 0; i != 10; i++ {
		msg, err := conn.ReadMessage(128)
		if err != nil {
			t.Error(err)
			continue
		}
		s := string(msg.Value)
		if v, err := strconv.Atoi(s); err != nil {
			t.Error(err)
		} else if v != i {
			t.Errorf("bad message read at offset %d: %s", i, s)
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

func waitForCoordinator(t *testing.T, conn *Conn, groupID string) {
	// ensure that kafka has allocated a group coordinator.  oddly, issue doesn't
	// appear to happen if the kafka been running for a while.
	const maxAttempts = 20
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		_, err := conn.findCoordinator(findCoordinatorRequestV1{
			CoordinatorKey: groupID,
		})
		switch err {
		case nil:
			return
		case GroupCoordinatorNotAvailable:
			time.Sleep(250 * time.Millisecond)
		default:
			t.Fatalf("unable to find coordinator for group: %v", err)
		}
	}

	t.Fatalf("unable to connect to coordinator after %v attempts", maxAttempts)
}

func createGroup(t *testing.T, conn *Conn, groupID string) (generationID int32, memberID string, stop func()) {
	waitForCoordinator(t, conn, groupID)

	join := func() (joinGroup joinGroupResponseV2) {
		var err error
		for attempt := 0; attempt < 10; attempt++ {
			joinGroup, err = conn.joinGroup(joinGroupRequestV2{
				GroupID:          groupID,
				SessionTimeout:   int32(time.Minute / time.Millisecond),
				RebalanceTimeout: int32(time.Second / time.Millisecond),
				ProtocolType:     "roundrobin",
				GroupProtocols: []joinGroupRequestGroupProtocolV2{
					{
						ProtocolName:     "roundrobin",
						ProtocolMetadata: []byte("blah"),
					},
				},
			})
			switch err {
			case nil:
				return
			case NotCoordinatorForGroup:
				time.Sleep(250 * time.Millisecond)
			default:
				t.Fatalf("bad joinGroup: %s", err)
			}
		}
		return
	}

	// join the group
	joinGroup := join()

	// sync the group
	_, err := conn.syncGroups(syncGroupRequestV1{
		GroupID:      groupID,
		GenerationID: joinGroup.GenerationID,
		MemberID:     joinGroup.MemberID,
		GroupAssignments: []syncGroupRequestGroupAssignmentV1{
			{
				MemberID:          joinGroup.MemberID,
				MemberAssignments: []byte("blah"),
			},
		},
	})
	if err != nil {
		t.Fatalf("bad syncGroups: %s", err)
	}

	generationID = joinGroup.GenerationID
	memberID = joinGroup.MemberID
	stop = func() {
		conn.leaveGroup(leaveGroupRequestV1{
			GroupID:  groupID,
			MemberID: joinGroup.MemberID,
		})
	}

	return
}

func testConnDescribeGroupRetrievesAllGroups(t *testing.T, conn *Conn) {
	groupID := makeGroupID()
	_, _, stop1 := createGroup(t, conn, groupID)
	defer stop1()

	out, err := conn.describeGroups(describeGroupsRequestV1{
		GroupIDs: []string{groupID},
	})
	if err != nil {
		t.Fatalf("bad describeGroups: %s", err)
	}

	if v := len(out.Groups); v != 1 {
		t.Fatalf("expected 1 group, got %v", v)
	}
	if id := out.Groups[0].GroupID; id != groupID {
		t.Errorf("bad group: got %v, expected %v", id, groupID)
	}
}

func testConnFindCoordinator(t *testing.T, conn *Conn) {
	groupID := makeGroupID()

	for attempt := 0; attempt < 10; attempt++ {
		if attempt != 0 {
			time.Sleep(time.Millisecond * 50)
		}
		response, err := conn.findCoordinator(findCoordinatorRequestV1{CoordinatorKey: groupID})
		if err != nil {
			switch err {
			case GroupCoordinatorNotAvailable:
				continue
			default:
				t.Fatalf("bad findCoordinator: %s", err)
			}
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
	_, err := conn.joinGroup(joinGroupRequestV2{})
	if err != InvalidGroupId && err != NotCoordinatorForGroup {
		t.Fatalf("expected %v or %v; got %v", InvalidGroupId, NotCoordinatorForGroup, err)
	}
}

func testConnJoinGroupInvalidSessionTimeout(t *testing.T, conn *Conn) {
	groupID := makeGroupID()
	waitForCoordinator(t, conn, groupID)

	_, err := conn.joinGroup(joinGroupRequestV2{
		GroupID: groupID,
	})
	if err != InvalidSessionTimeout && err != NotCoordinatorForGroup {
		t.Fatalf("expected %v or %v; got %v", InvalidSessionTimeout, NotCoordinatorForGroup, err)
	}
}

func testConnJoinGroupInvalidRefreshTimeout(t *testing.T, conn *Conn) {
	groupID := makeGroupID()
	waitForCoordinator(t, conn, groupID)

	_, err := conn.joinGroup(joinGroupRequestV2{
		GroupID:        groupID,
		SessionTimeout: int32(3 * time.Second / time.Millisecond),
	})
	if err != InvalidSessionTimeout && err != NotCoordinatorForGroup {
		t.Fatalf("expected %v or %v; got %v", InvalidSessionTimeout, NotCoordinatorForGroup, err)
	}
}

func testConnHeartbeatErr(t *testing.T, conn *Conn) {
	groupID := makeGroupID()
	createGroup(t, conn, groupID)

	_, err := conn.syncGroups(syncGroupRequestV1{
		GroupID: groupID,
	})
	if err != UnknownMemberId && err != NotCoordinatorForGroup {
		t.Fatalf("expected %v or %v; got %v", UnknownMemberId, NotCoordinatorForGroup, err)
	}
}

func testConnLeaveGroupErr(t *testing.T, conn *Conn) {
	groupID := makeGroupID()
	waitForCoordinator(t, conn, groupID)

	_, err := conn.leaveGroup(leaveGroupRequestV1{
		GroupID: groupID,
	})
	if err != UnknownMemberId && err != NotCoordinatorForGroup {
		t.Fatalf("expected %v or %v; got %v", UnknownMemberId, NotCoordinatorForGroup, err)
	}
}

func testConnSyncGroupErr(t *testing.T, conn *Conn) {
	groupID := makeGroupID()
	waitForCoordinator(t, conn, groupID)

	_, err := conn.syncGroups(syncGroupRequestV1{
		GroupID: groupID,
	})
	if err != UnknownMemberId && err != NotCoordinatorForGroup {
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

	request := offsetFetchRequestV3{
		GroupID: groupID,
		Topics: []offsetFetchRequestV3Topic{
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
	_, err = conn.offsetCommit(offsetCommitRequestV3{
		GroupID:       groupID,
		GenerationID:  generationID,
		MemberID:      memberID,
		RetentionTime: int64(time.Hour / time.Millisecond),
		Topics: []offsetCommitRequestV3Topic{
			{
				Topic: conn.topic,
				Partitions: []offsetCommitRequestV3Partition{
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
	var msgs = make([]string, N)
	var done = make(chan struct{})

	for i := 0; i != N; i++ {
		msgs[i] = strconv.Itoa(i)
	}

	go func() {
		defer close(done)
		for _, msg := range msgs {
			if _, err := conn.Write([]byte(msg)); err != nil {
				t.Error(err)
			}
		}
	}()

	b := make([]byte, 128)

	for i := 0; i != N; i++ {
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
		if err != io.ErrShortBuffer {
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
	deadline := start.Add(100 * time.Millisecond)

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
	err = conn.DeleteTopics("invalid-topic", topic)
	if err != UnknownTopicOrPartition {
		t.Fatalf("expected UnknownTopicOrPartition error, but got %v", err)
	}
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		t.Fatalf("bad ReadPartitions: %v", err)
	}
	if len(partitions) != 0 {
		t.Fatal("exepected partitions to be empty")
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
