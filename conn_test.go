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

	offset, err := conn.Seek(0, 0)
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

	offset, err := conn.Seek(0, 2)
	if err != nil {
		t.Error(err)
	}

	if offset != 10 {
		t.Error("bad offset:", offset)
	}
}

func testConnSeekRandomOffset(t *testing.T, conn *Conn) {
	for i := 0; i != 10; i++ {
		if _, err := conn.Write([]byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	offset, err := conn.Seek(3, 1)
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
			if _, err := conn.Seek(0, 0); err != nil {
				b.Error(err)
				return
			}
			benchmark.function(b, conn, value)
		})
	}
}

func benchmarkConnSeek(b *testing.B, conn *Conn, _ []byte) {
	for i := 0; i != b.N; i++ {
		if _, err := conn.Seek(int64(i%benchmarkMessageCount), 1); err != nil {
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
			if _, err := conn.Seek(0, 0); err != nil {
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
			if _, err = conn.Seek(0, 0); err != nil {
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
