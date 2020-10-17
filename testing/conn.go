package testing

import (
	"context"
	"net"
	"sync"
)

type ConnWaitGroup struct {
	DialFunc func(context.Context, string, string) (net.Conn, error)
	sync.WaitGroup
}

func (g *ConnWaitGroup) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	c, err := g.DialFunc(ctx, network, address)
	if err != nil {
		return nil, err
	}
	g.Add(1)
	return &groupConn{Conn: c, group: g}, nil
}

type groupConn struct {
	net.Conn
	group *ConnWaitGroup
	once  sync.Once
}

func (c *groupConn) Close() error {
	defer c.once.Do(c.group.Done)
	return c.Conn.Close()
}
