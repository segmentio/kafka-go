package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"
)

type Conn struct {
	buffer   *bufio.Reader
	conn     net.Conn
	clientID string
	idgen    int32
	versions atomic.Value // map[ApiKey]int16
}

func NewConn(conn net.Conn, clientID string) *Conn {
	return &Conn{
		buffer:   bufio.NewReader(conn),
		conn:     conn,
		clientID: clientID,
	}
}

func (c *Conn) String() string {
	return fmt.Sprintf("kafka://%s@%s->%s", c.clientID, c.LocalAddr(), c.RemoteAddr())
}

func (c *Conn) Close() error {
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("could not close kafka connection: %w", err)
	}
	return nil
}

func (c *Conn) Discard(n int) (int, error) {
	n, err := c.buffer.Discard(n)
	if err != nil {
		return n, fmt.Errorf("kafka connection discard failed: %w", err)
	}
	return n, nil
}

func (c *Conn) Peek(n int) ([]byte, error) {
	data, err := c.buffer.Peek(n)
	if err != nil {
		return data, fmt.Errorf("kakfa connection peek failed: %w", err)
	}
	return data, nil
}

func (c *Conn) Read(b []byte) (int, error) {
	n, err := c.buffer.Read(b)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return n, io.EOF
		}

		return n, fmt.Errorf("kafka connection read failed: %w", err)
	}
	return n, nil
}

func (c *Conn) Write(b []byte) (int, error) {
	n, err := c.conn.Write(b)
	if err != nil {
		return n, fmt.Errorf("kafka connection write failed: %w", err)
	}
	return n, nil
}

func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Conn) SetDeadline(t time.Time) error {
	if err := c.conn.SetDeadline(t); err != nil {
		return fmt.Errorf("kafka connection set deadline failed: %w", err)
	}

	return nil
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	if err := c.conn.SetReadDeadline(t); err != nil {
		return fmt.Errorf("kafka connection set read deadline failed: %w", err)
	}

	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	if err := c.conn.SetWriteDeadline(t); err != nil {
		return fmt.Errorf("kafka connection set write deadline failed: %w", err)
	}

	return nil
}

func (c *Conn) SetVersions(versions map[ApiKey]int16) {
	connVersions := make(map[ApiKey]int16, len(versions))

	for k, v := range versions {
		connVersions[k] = v
	}

	c.versions.Store(connVersions)
}

func (c *Conn) RoundTrip(msg Message) (Message, error) {
	correlationID := atomic.AddInt32(&c.idgen, +1)
	versions, _ := c.versions.Load().(map[ApiKey]int16)
	apiVersion := versions[msg.ApiKey()]

	if p, _ := msg.(PreparedMessage); p != nil {
		p.Prepare(apiVersion)
	}

	if raw, ok := msg.(RawExchanger); ok && raw.Required(versions) {
		msg, err := raw.RawExchange(c)
		if err != nil {
			return msg, fmt.Errorf("raw exchange failed: %w", err)
		}
		return msg, nil
	}

	return RoundTrip(c, apiVersion, correlationID, c.clientID, msg)
}

var (
	_ net.Conn       = (*Conn)(nil)
	_ bufferedReader = (*Conn)(nil)
)
