package kafka

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/apiversions"
	meta "github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/segmentio/kafka-go/protocol/saslauthenticate"
	"github.com/segmentio/kafka-go/protocol/saslhandshake"
	"github.com/segmentio/kafka-go/sasl"
)

// RoundTripper is an interface implemented by types which support interacting
// with kafka brokers.
type RoundTripper interface {
	// RoundTrip sends a request to a kafka broker and returns the response that
	// was received, or a non-nil error.
	//
	// The context passed as first argument is used to asynchronnously abort the
	// request.
	RoundTrip(context.Context, protocol.Message) (protocol.Message, error)
}

// Transport is an implementation of the RoundTripper interface.
//
// Transport values manage a pool of connections and automatically discovers the
// cluster layout to manage to dispatch produce and fetch requests to the
// partition leaders.
//
// Transport values are safe to use concurrently from multiple goroutines.
//
// Note: The intent is for the Transport to become the underlying layer of the
// kafka.Reader and kafka.Writer types.
type Transport struct {
	// Address of the kafka cluster that the transport will be sending requests
	// to. If a program needs to interact with multiple kafka clusters, it needs
	// to create one transport for each cluster.
	Addr string

	// A function used to establish connections to the kafka cluster.
	Dial func(context.Context, string, string) (net.Conn, error)

	// Time limit set for establishing connections to the kafka cluster.
	//
	// Defaults to 5s.
	DialTimeout time.Duration

	// Maximum amount of time that the connections will wait for responses from
	// the kafka cluster. Be mindful that this value should be longer than the
	// duration comunicated in some messages (like the fetch MaxWait value for
	// example).
	//
	// Default to 10s.
	ReadTimeout time.Duration

	// Maximum amount of time that connections will remain open and unused.
	// The transport will manage to automatically close connections that have
	// been idle for too long, and re-open them on demand when the transport is
	// used again.
	//
	// Defaults to 30s.
	IdleTimeout time.Duration

	// Time interval between updates of the cluster metadata.
	//
	// Default to 2s.
	MetadataUpdateInterval time.Duration

	// Unique identifier that the transport communicates to the brokers when it
	// sends requests.
	ClientID string

	// If non-nil, the transport establishes TLS connections to the cluster.
	TLS *tls.Config

	// SASL configures the Transfer to use SASL authentication.
	SASL sasl.Mechanism

	mutex  sync.RWMutex
	once   sync.Once
	ready  chan struct{}
	done   chan struct{}
	ctrl   *conn
	conns  map[int]*conn
	layout protocol.Cluster
	err    error
}

// CloseIdleConnections closes all idle connections immediately, and marks all
// connections that are in use to be closed when they become idle again.
func (t *Transport) CloseIdleConnections() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	for _, conn := range t.conns {
		conn.unref()
	}

	for id := range t.conns {
		delete(t.conns, id)
	}

	if t.done != nil {
		close(t.done)
		t.ctrl = nil
		t.done = nil
	}
}

// RoundTrip sends a request to the kafka broker that c is connected to, and
// returns the response, or an error if no response could be received.
//
// Message types are available in sub-packages of the protocol package. Each
// kafka API is implemented in a different sub-package. For example, the request
// and response types for the Fetch API are available in the protocol/fetch
// package.
//
// The type of the response message will match the type of the request. For
// exmple, if RoundTrip was called with a *fetch.Request as argument, the value
// returned will be of type *fetch.Response. It is safe for the program to do a
// type assertion after checking that no error was returned.
//
// This example illustrates the way this method is expected to be used:
//
//	r, err := transport.RoundTrip(ctx, &fetch.Request{ ... })
//  if err != nil {
//		...
//	} else {
//	    res := r.(*fetch.Response)
//	    ...
//	}
//
// The transport automatically selects the highest version of the API that is
// supported by both the kafka-go package and the kafka broker. The negotiation
// happens transparently once when connections are established.
//
// This API was introduced in version 0.4 as a way to leverage the lower-level
// features of the kafka protocol, but also provide a more efficient way of
// managing connections to kafka brokers.
func (t *Transport) RoundTrip(ctx context.Context, req protocol.Message) (protocol.Message, error) {
	t.init()
	canceled := ctx.Done()

	select {
	case <-t.ready:
	case <-canceled:
		return nil, ctx.Err()
	}

	c, err := t.grabConn(req)
	if err != nil {
		return nil, err
	}

	defer c.unref()
	res := make(chan connResponse, 1)

	select {
	case c.reqs <- connRequest{
		req: req,
		res: res,
	}:
	case <-canceled:
		return nil, ctx.Err()
	}

	select {
	case r := <-res:
		return r.res, r.err
	case <-canceled:
		return nil, ctx.Err()
	}
}

// Layout returns the cluster layout known to t.
//
// The transport caches the layout, so calling this method frequently should not
// add any overhead to the program. A context is still expected for asynchronous
// cancellation when the transport cache is still empty.
func (t *Transport) Layout(ctx context.Context) (protocol.Cluster, error) {
	t.init()
	canceled := ctx.Done()

	select {
	case <-t.ready:
	case <-canceled:
		return protocol.Cluster{}, ctx.Err()
	}

	t.mutex.RLock()
	layout, err := t.layout, t.err
	t.mutex.RUnlock()
	return layout, err
}

func (t *Transport) dial() func(context.Context, string, string) (net.Conn, error) {
	if t.Dial != nil {
		return t.Dial
	}
	return defaultDialer.DialContext
}

func (t *Transport) dialTimeout() time.Duration {
	if t.DialTimeout > 0 {
		return t.DialTimeout
	}
	return 5 * time.Second
}

func (t *Transport) readTimeout() time.Duration {
	if t.ReadTimeout != 0 {
		return t.ReadTimeout
	}
	return 10 * time.Second
}

func (t *Transport) idleTimeout() time.Duration {
	if t.IdleTimeout > 0 {
		return t.IdleTimeout
	}
	return 30 * time.Second
}

func (t *Transport) metadataUpdateInterval() time.Duration {
	if t.MetadataUpdateInterval > 0 {
		return t.MetadataUpdateInterval
	}
	return 2 * time.Second
}

func (t *Transport) init() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.ready == nil {
		t.ready = make(chan struct{})
	}

	if t.conns == nil {
		t.conns = make(map[int]*conn)
	}

	if t.ctrl == nil {
		t.ctrl = t.connect("tcp", t.Addr)
		t.done = make(chan struct{})
		go t.discover(t.ctrl, t.done)
	}
}

func (t *Transport) setReady() {
	t.once.Do(func() { close(t.ready) })
}

// grabConn returns the connection which should serve the request passed as
// argument. If no connections were available, an error is returned.
//
// The transport uses a shared `control` connection to the cluster for any
// requests that aren't supposed to be sent to specific brokers (e.g. Fetch or
// Produce requests). Requests intended to be routed to specific brokers are
// dispatched on a separate pool of connections that the transport maintains.
// This split help avoid head-of-line blocking situations where control requests
// like Metadata would be queued behind large responses from Fetch requests for
// example.
//
// In either cases, the requests are multiplexed so we can keep a minimal number
// of connections open (N+1, where N is the number of brokers in the cluster).
func (t *Transport) grabConn(req protocol.Message) (*conn, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	layout, err := t.layout, t.err
	if err != nil {
		return nil, err
	}

	var c *conn
	switch m := req.(type) {
	case protocol.BrokerMessage:
		broker, err := m.Broker(layout)
		if err != nil {
			return nil, err
		}
		c = t.conns[broker.ID]
	default:
		c = t.ctrl
	}

	c.ref()
	return c, nil
}

// update is called periodically by the goroutine running the discover method
// to refresh the cluster layout information used by the transport to route
// requests to brokers.
func (t *Transport) update(layout protocol.Cluster, err error) {
	t.mutex.Lock()
	defer t.setReady()
	defer t.mutex.Unlock()

	if err != nil {
		// Only update the error on the transport if the cluster layout was
		// unknown. This ensures that we prioritize a previously known state
		// of the cluster to reduce the impact of transient failures.
		if t.layout.IsZero() {
			t.err = err
		}
		return
	}

	// We got a successful update, we will be using this state from now on,
	// and won't report errors seen trying to refresh the cluster layout
	// state. If the state gets out of sync, some requests may fail
	// (e.g. Produce requests going to brokers that are not leading a
	// partition), the program is expected to handle these errors and retry
	// accordingly.
	t.layout, t.err = layout, nil

	for id, broker := range t.layout.Brokers {
		if _, ok := t.conns[id]; !ok {
			t.conns[id] = t.connect(broker.Network(), broker.String())
		}
	}

	for id, conn := range t.conns {
		if _, ok := t.layout.Brokers[id]; !ok {
			conn.unref()
			delete(t.conns, id)
		}
	}
}

// discover is the entry point of an internal goroutine for the transport which
// periodically requests updates of the cluster metadata and refreshes the
// transport cached cluster layout.
func (t *Transport) discover(conn *conn, done <-chan struct{}) {
	defer conn.unref()

	ticker := time.NewTicker(t.metadataUpdateInterval())
	defer ticker.Stop()

	res := make(chan connResponse, 1)

	for {
		select {
		case conn.reqs <- connRequest{
			req: new(meta.Request),
			res: res,
		}:
		case <-done:
			return
		}

		var r connResponse
		select {
		case r = <-res:
		case <-done:
			return
		}

		if r.err != nil {
			t.update(protocol.Cluster{}, r.err)
		} else {
			t.update(makeLayout(r.res.(*meta.Response)), nil)
		}

		select {
		case <-ticker.C:
		case <-done:
			return
		}
	}
}

func makeLayout(metadataResponse *meta.Response) protocol.Cluster {
	layout := protocol.Cluster{
		Controller: int(metadataResponse.ControllerID),
		Brokers:    make(map[int]protocol.Broker),
		Topics:     make(map[string]protocol.Topic),
	}

	for _, broker := range metadataResponse.Brokers {
		layout.Brokers[int(broker.NodeID)] = protocol.Broker{
			Rack: broker.Rack,
			Host: broker.Host,
			Port: int(broker.Port),
			ID:   int(broker.NodeID),
		}
	}

	for _, topic := range metadataResponse.Topics {
		if topic.IsInternal {
			continue // TODO: do we need to expose those?
		}
		layout.Topics[topic.Name] = protocol.Topic{
			Name:       topic.Name,
			Error:      int(topic.ErrorCode),
			Partitions: makePartitions(topic.Partitions),
		}
	}

	return layout
}

func makePartitions(metadataPartitions []meta.ResponsePartition) map[int]protocol.Partition {
	protocolPartitions := make(map[int]protocol.Partition, len(metadataPartitions))

	for _, p := range metadataPartitions {
		protocolPartitions[int(p.PartitionIndex)] = protocol.Partition{
			ID:     int(p.PartitionIndex),
			Error:  int(p.ErrorCode),
			Leader: int(p.LeaderID),
		}
	}

	return protocolPartitions
}

type connRequest struct {
	req protocol.Message
	res chan<- connResponse
	// Lazily negotiated when writing the request, then reused to determine the
	// version of the message we're reading in the response.
	version int16
}

type connResponse struct {
	res protocol.Message
	err error
}

const (
	// Size of the read and write buffers on the connection. We use a larger
	// value than the default 8KB of the bufio package to reduce the number of
	// syscalls needed to exchange the (often large) kafka messages with the
	// kernel.
	bufferSize = 32 * 1024
)

var (
	// Default dialer used by the transport connections when no Dial function
	// was configured by the program.
	defaultDialer = net.Dialer{
		DualStack: true,
	}
)

// conn represents a logical connection to a kafka broker. The actual network
// connections are lazily open before sending requests, and closed if they are
// unused for longer than the idle timeout.
type conn struct {
	// Reference counter used to orchestrate graceful shutdown.
	refc uintptr
	// Immutable state of the connection.
	reqs        chan<- connRequest
	network     string
	address     string
	dial        func(context.Context, string, string) (net.Conn, error)
	dialTimeout time.Duration
	readTimeout time.Duration
	idleTimeout time.Duration
	clientID    string
	tls         *tls.Config
	sasl        sasl.Mechanism
	// Shared state of the connection, this is synchronized on the mutex through
	// calls to the synchronized method. Both goroutines of the connection share
	// the state maintained in these fields.
	mutex    sync.Mutex
	idgen    int32                 // generates new correlation ids
	recycled map[int32]struct{}    // correlation ids available for reuse
	inflight map[int32]connRequest // set of inflight requests on the connection
}

func (t *Transport) connect(network, address string) *conn {
	reqs := make(chan connRequest)
	conn := &conn{
		refc:        1,
		reqs:        reqs,
		network:     network,
		address:     address,
		dial:        t.dial(),
		dialTimeout: t.dialTimeout(),
		readTimeout: t.readTimeout(),
		idleTimeout: t.idleTimeout(),
		clientID:    t.ClientID,
		tls:         t.TLS,
		sasl:        t.SASL,
		recycled:    make(map[int32]struct{}),
		inflight:    make(map[int32]connRequest),
	}
	go conn.run(reqs)
	return conn
}

func (c *conn) ref() {
	atomic.AddUintptr(&c.refc, 1)
}

func (c *conn) unref() {
	if atomic.AddUintptr(&c.refc, ^uintptr(0)) == 0 {
		close(c.reqs)
	}
}

func (c *conn) register(r connRequest) (id int32) {
	c.synchronized(func() {
		var ok bool

		for id = range c.recycled {
			ok = true
			break
		}

		if !ok {
			c.idgen++
			id = c.idgen
		}

		c.inflight[id] = r
	})
	return
}

func (c *conn) unregister(id int32) (r connRequest, ok bool) {
	c.synchronized(func() {
		r, ok = c.inflight[id]
		if ok {
			delete(c.inflight, id)
			c.recycled[id] = struct{}{}
		}
	})
	return
}

func (c *conn) cancel(err error) {
	c.synchronized(func() {
		for id, r := range c.inflight {
			r.res <- connResponse{err: err}
			delete(c.inflight, id)
			c.recycled[id] = struct{}{}
		}
	})
}

func (c *conn) synchronized(f func()) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	f()
}

func (c *conn) connect() (net.Conn, *bufio.ReadWriter, map[protocol.ApiKey]int16, error) {
	deadline := time.Now().Add(c.dialTimeout)

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	netConn, err := c.dial(ctx, c.network, c.address)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() {
		if netConn != nil {
			netConn.Close()
		}
	}()

	if c.tls != nil {
		netConn = tls.Client(netConn, c.tls)
	}

	nc := netConn
	rw := &bufio.ReadWriter{
		Reader: bufio.NewReaderSize(netConn, bufferSize),
		Writer: bufio.NewWriterSize(netConn, bufferSize),
	}

	if err := nc.SetDeadline(deadline); err != nil {
		return nil, nil, nil, err
	}

	if c.sasl != nil {
		if err := c.authenticateSASL(ctx, rw, c.sasl); err != nil {
			return nil, nil, nil, err
		}
	}

	r, err := protocol.RoundTrip(rw, v0, c.clientID, new(apiversions.Request))
	if err != nil {
		return nil, nil, nil, err
	}
	res := r.(*apiversions.Response)
	ver := make(map[protocol.ApiKey]int16, len(res.ApiKeys))

	if res.ErrorCode != 0 {
		return nil, nil, nil, fmt.Errorf("negotating API versions with kafka broker at %s: %w", c.address, Error(res.ErrorCode))
	}

	for _, r := range res.ApiKeys {
		apiKey := protocol.ApiKey(r.ApiKey)
		ver[apiKey] = apiKey.SelectVersion(r.MinVersion, r.MaxVersion)
	}

	if err := nc.SetDeadline(time.Now().Add(c.idleTimeout)); err != nil {
		return nil, nil, nil, err
	}

	netConn = nil
	return nc, rw, ver, nil
}

func (c *conn) dispatch(conn net.Conn, done chan<- error, rb *bufio.Reader) {
	for {
		_, id, err := peekResponseSizeAndID(rb)
		if err != nil {
			done <- err
			return
		}

		r, ok := c.unregister(id)
		if !ok {
			// Technically we could just skip the number of bytes read by
			// peekResponseSizeAndID, but that seems risky. If the server
			// sent an unexpected message, just abort.
			done <- fmt.Errorf("received response from %s for unknown correlation id %d", conn.RemoteAddr(), id)
			return
		}

		_, res, err := protocol.ReadResponse(rb, int16(r.req.ApiKey()), r.version)

		r.res <- connResponse{
			res: res,
			err: err,
		}

		if err != nil {
			done <- err
			return
		}

		c.synchronized(func() {
			if len(c.inflight) == 0 {
				conn.SetDeadline(time.Now().Add(c.idleTimeout))
			}
		})
	}
}

func (c *conn) run(reqs <-chan connRequest) {
	var conn net.Conn
	var done chan error
	var wb *bufio.Writer
	var versions map[protocol.ApiKey]int16

	closeConn := func(err error) {
		if conn != nil {
			conn.Close()
			conn = nil
		}
		done = nil
		c.cancel(err)
	}
	defer closeConn(io.ErrClosedPipe)

	for {
		select {
		case r, ok := <-reqs:
			if !ok {
				return
			}

			if conn == nil {
				var rw *bufio.ReadWriter
				var err error

				conn, rw, versions, err = c.connect()
				if err != nil {
					r.res <- connResponse{err: err}
					continue
				}

				wb = rw.Writer
				done = make(chan error, 1)
				go c.dispatch(conn, done, rw.Reader)
			}

			r.version = versions[r.req.ApiKey()]
			id := c.register(r)
			conn.SetDeadline(time.Now().Add(c.readTimeout))

			if err := protocol.WriteRequest(wb, r.version, id, c.clientID, r.req); err != nil {
				closeConn(err)
			}

		case err := <-done:
			closeConn(err)
		}
	}
}

func peekResponseSizeAndID(rb *bufio.Reader) (int32, int32, error) {
	b, err := rb.Peek(8)
	if err != nil {
		return 0, 0, err
	}
	size, id := makeInt32(b[:4]), makeInt32(b[4:])
	return size, id, nil
}

// authenticateSASL performs all of the required requests to authenticate this
// connection.  If any step fails, this function returns with an error.  A nil
// error indicates successful authentication.
//
// In case of error, this function *does not* close the connection.  That is the
// responsibility of the caller.
func (c *conn) authenticateSASL(ctx context.Context, rw *bufio.ReadWriter, mechanism sasl.Mechanism) error {
	if err := c.saslHandshake(rw, mechanism.Name()); err != nil {
		return err
	}

	sess, state, err := mechanism.Start(ctx)
	if err != nil {
		return err
	}

	for completed := false; !completed; {
		challenge, err := c.saslAuthenticate(rw, state)
		switch err {
		case nil:
		case io.EOF:
			// the broker may communicate a failed exchange by closing the
			// connection (esp. in the case where we're passing opaque sasl
			// data over the wire since there's no protocol info).
			return SASLAuthenticationFailed
		default:
			return err
		}

		completed, state, err = sess.Next(ctx, challenge)
		if err != nil {
			return err
		}
	}

	return nil
}

// saslHandshake sends the SASL handshake message.  This will determine whether
// the Mechanism is supported by the cluster.  If it's not, this function will
// error out with UnsupportedSASLMechanism.
//
// If the mechanism is unsupported, the handshake request will reply with the
// list of the cluster's configured mechanisms, which could potentially be used
// to facilitate negotiation.  At the moment, we are not negotiating the
// mechanism as we believe that brokers are usually known to the client, and
// therefore the client should already know which mechanisms are supported.
//
// See http://kafka.apache.org/protocol.html#The_Messages_SaslHandshake
func (c *conn) saslHandshake(rw *bufio.ReadWriter, mechanism string) error {
	msg, err := protocol.RoundTrip(rw, v0, c.clientID, &saslhandshake.Request{
		Mechanism: mechanism,
	})
	if err != nil {
		return err
	}
	res := msg.(*saslhandshake.Response)
	if res.ErrorCode != 0 {
		err = Error(res.ErrorCode)
	}
	return err
}

// saslAuthenticate sends the SASL authenticate message.  This function must
// be immediately preceded by a successful saslHandshake.
//
// See http://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate
func (c *conn) saslAuthenticate(rw *bufio.ReadWriter, data []byte) ([]byte, error) {
	msg, err := protocol.RoundTrip(rw, v1, c.clientID, &saslauthenticate.Request{
		AuthBytes: data,
	})
	if err != nil {
		return nil, err
	}
	res := msg.(*saslauthenticate.Response)
	if res.ErrorCode != 0 {
		err = Error(res.ErrorCode)
	}
	return res.AuthBytes, err
}

var (
	_ RoundTripper = (*Transport)(nil)
)
