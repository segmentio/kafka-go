package kafka

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"
)

// The Dialer type mirrors the net.Dialer API but is designed to open kafka
// connections instead of raw network connections.
type Dialer struct {
	// Unique identifier for client connections established by this Dialer.
	ClientID string

	// Name of the default kafka topic configured on connections established
	// through the Dialer.
	Topic string

	// The partition number used by the connections returned by this dialer.
	Partition int

	// Timeout is the maximum amount of time a dial will wait for a connect to
	// complete. If Deadline is also set, it may fail earlier.
	//
	// The default is no timeout.
	//
	// When dialing a name with multiple IP addresses, the timeout may be
	// divided between them.
	//
	// With or without a timeout, the operating system may impose its own
	// earlier timeout. For instance, TCP timeouts are often around 3 minutes.
	Timeout time.Duration

	// Deadline is the absolute point in time after which dials will fail.
	// If Timeout is set, it may fail earlier.
	// Zero means no deadline, or dependent on the operating system as with the
	// Timeout option.
	Deadline time.Time

	// LocalAddr is the local address to use when dialing an address.
	// The address must be of a compatible type for the network being dialed.
	// If nil, a local address is automatically chosen.
	LocalAddr net.Addr

	// DualStack enables RFC 6555-compliant "Happy Eyeballs" dialing when the
	// network is "tcp" and the destination is a host name with both IPv4 and
	// IPv6 addresses. This allows a client to tolerate networks where one
	// address family is silently broken.
	DualStack bool

	// FallbackDelay specifies the length of time to wait before spawning a
	// fallback connection, when DualStack is enabled.
	// If zero, a default delay of 300ms is used.
	FallbackDelay time.Duration

	// KeepAlive specifies the keep-alive period for an active network
	// connection.
	// If zero, keep-alives are not enabled. Network protocols that do not
	// support keep-alives ignore this field.
	KeepAlive time.Duration

	// Resolver optionally specifies an alternate resolver to use.
	Resolver *net.Resolver
}

// Dial connects to the address on the named network.
func (d *Dialer) Dial(network string, address string) (*Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

// DialContext connects to the address on the named network using the provided
// context.
//
// If the dialer's Topic is set, the address given to the DialContext method may
// not be the one that the connection will end up being established to, because
// the dialer will lookup the partition leader for the topic and return a
// connection to that server. The original address is only used as a mechanism
// to discover the configuration of the kafka cluster that we're connecting to.
//
// The provided Context must be non-nil. If the context expires before the
// connection is complete, an error is returned. Once successfully connected,
// any expiration of the context will not affect the connection.
//
// When using TCP, and the host in the address parameter resolves to multiple
// network addresses, any dial timeout (from d.Timeout or ctx) is spread over
// each consecutive dial, such that each is given an appropriate fraction of the
//time to connect. For example, if a host has 4 IP addresses and the timeout is
// 1 minute, the connect to each single address will be given 15 seconds to
// complete before trying the next one.
func (d *Dialer) DialContext(ctx context.Context, network string, address string) (*Conn, error) {
	if d.Timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.Timeout)
		defer cancel()
	}

	if !d.Deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, d.Deadline)
		defer cancel()
	}

	dialer := net.Dialer{
		LocalAddr:     d.LocalAddr,
		DualStack:     d.DualStack,
		FallbackDelay: d.FallbackDelay,
		KeepAlive:     d.KeepAlive,
		Resolver:      d.Resolver,
	}

	c, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}

	conn := NewConnWith(c, ConnConfig{ClientID: d.ClientID})
	if len(d.Topic) == 0 {
		return conn, nil
	}

	resch := make(chan string, 1)
	errch := make(chan error, 1)

	go func() {
		var paritions []Partition
		var err error

		for attempt := 1; true; attempt++ {
			paritions, err = conn.ReadPartitions()
			switch {
			case isTemporary(err):
				sleep(ctx, backoff(attempt, time.Second, time.Minute))
			case err != nil:
				errch <- err
				return
			}
		}

		for _, p := range paritions {
			if p.ID == d.Partition {
				resch <- net.JoinHostPort(p.Leader.Host, strconv.Itoa(p.Leader.Port))
				return
			}
		}

		errch <- fmt.Errorf("kafka topic '%s' has no partition with id '%d'", d.Topic, d.Partition)
	}()

	select {
	case address = <-resch:
	case err = <-errch:
	case <-ctx.Done():
		err = ctx.Err()
	}

	if err != nil {
		conn.Close()
		return nil, err
	}

	c, err = dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}

	conn = NewConnWith(c, ConnConfig{
		ClientID:  d.ClientID,
		Topic:     d.Topic,
		Partition: d.Partition,
	})
	return conn, nil
}

func sleep(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
	timer.Stop()
}

func backoff(attempt int, min time.Duration, max time.Duration) time.Duration {
	d := time.Duration(attempt*attempt) * min
	if d > max {
		d = max
	}
	return d
}
