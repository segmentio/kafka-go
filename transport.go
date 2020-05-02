package kafka

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/apiversions"
	"github.com/segmentio/kafka-go/protocol/createtopics"
	"github.com/segmentio/kafka-go/protocol/deletetopics"
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
	// The context passed as first argument can be used to asynchronnously abort
	// the call if needed.
	RoundTrip(context.Context, net.Addr, protocol.Message) (protocol.Message, error)
}

// Transport is an implementation of the RoundTripper interface.
//
// Transport values manage a pool of connections and automatically discovers the
// clusters layout to manage to dispatch produce and fetch requests to the
// appropriate brokers.
//
// Transport values are safe to use concurrently from multiple goroutines.
//
// Note: The intent is for the Transport to become the underlying layer of the
// kafka.Reader and kafka.Writer types.
type Transport struct {
	// A function used to establish connections to the kafka cluster.
	Dial func(context.Context, string, string) (net.Conn, error)

	// Time limit set for establishing connections to the kafka cluster. This
	// limit includes all round trips done to establish the connections (TLS
	// hadbhaske, SASL negotiation, etc...).
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

	// TTL for the metadata cached by this transport. Note that the value
	// configured here is an upper bound, the transport randomizes the TTLs to
	// avoid getting into states where multiple clients end up synchronized and
	// cause bursts of requests to the kafka broker.
	//
	// Default to 6s.
	MetadataTTL time.Duration

	// Unique identifier that the transport communicates to the brokers when it
	// sends requests.
	ClientID string

	// An optional configuration for TLS connections established by this
	// transport.
	//
	// If the Server
	TLS *tls.Config

	// SASL configures the Transfer to use SASL authentication.
	SASL sasl.Mechanism

	mutex sync.RWMutex
	pools map[networkAddress]*connPool
}

// DefaultTransport is the default transport used by kafka clients in this
// package.
var DefaultTransport RoundTripper = &Transport{
	Dial: (&net.Dialer{
		Timeout:   3 * time.Second,
		DualStack: true,
	}).DialContext,
}

// CloseIdleConnections closes all idle connections immediately, and marks all
// connections that are in use to be closed when they become idle again.
func (t *Transport) CloseIdleConnections() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	for _, pool := range t.pools {
		pool.closeIdleConnections()
	}

	for k := range t.pools {
		delete(t.pools, k)
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
//	r, err := transport.RoundTrip(ctx, addr, &fetch.Request{ ... })
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
func (t *Transport) RoundTrip(ctx context.Context, addr net.Addr, req protocol.Message) (protocol.Message, error) {
	return t.grabPool(addr).roundTrip(ctx, req)
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

func (t *Transport) metadataTTL() time.Duration {
	if t.MetadataTTL > 0 {
		return t.MetadataTTL
	}
	return 6 * time.Second
}

func (t *Transport) grabPool(addr net.Addr) *connPool {
	k := networkAddress{
		network: addr.Network(),
		address: addr.String(),
	}

	t.mutex.RLock()
	p := t.pools[k]
	t.mutex.RUnlock()

	if p != nil {
		return p
	}

	network := k.network
	address := k.address

	host, port, _ := net.SplitHostPort(address)
	if port == "" {
		port = "9092"
	}
	if host == "" {
		host = address
	}
	address = net.JoinHostPort(host, port)

	var tlsConfig *tls.Config
	if network == "tls" {
		network = "tcp"

		switch tlsConfig = t.TLS; {
		case tlsConfig == nil:
			tlsConfig = &tls.Config{ServerName: host}
		case tlsConfig.ServerName == "" && !tlsConfig.InsecureSkipVerify:
			tlsConfig = tlsConfig.Clone()
			tlsConfig.ServerName = host
		}
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	if p := t.pools[k]; p != nil {
		return p
	}

	p = &connPool{
		network:     network,
		address:     address,
		dial:        t.dial(),
		dialTimeout: t.dialTimeout(),
		readTimeout: t.readTimeout(),
		idleTimeout: t.idleTimeout(),
		metadataTTL: t.metadataTTL(),
		clientID:    t.ClientID,
		tls:         tlsConfig,
		sasl:        t.SASL,

		ready: make(event),
		done:  make(event),
		wake:  make(chan event),
		conns: make(map[int]*conn),
	}

	if t.pools == nil {
		t.pools = map[networkAddress]*connPool{k: p}
	} else {
		t.pools[k] = p
	}

	p.ctrl = p.connect(k.network, k.address)
	go p.discover(p.ctrl, p.wake, p.done)
	return p
}

type event chan struct{}

func (e event) trigger() { close(e) }

type connPool struct {
	// Immutable fields of the connection pool. Connections access these field
	// on their parent pool in a ready-only fashion, so no synchronization is
	// required.
	network     string
	address     string
	dial        func(context.Context, string, string) (net.Conn, error)
	dialTimeout time.Duration
	readTimeout time.Duration
	idleTimeout time.Duration
	metadataTTL time.Duration
	clientID    string
	tls         *tls.Config
	sasl        sasl.Mechanism
	// Signaling mechanisms to orchestrate communications between the pool and
	// the rest of the program.
	once  sync.Once  // ensure that `ready` is triggered only once
	ready event      // triggered after the first metadata update
	done  event      // triggered when the connection pool is closed
	wake  chan event // used to force metadata updates
	// Mutable fields of the connection pool, access must be synchronized.
	mutex    sync.RWMutex
	conns    map[int]*conn    // data connections used for produce/fetch/etc...
	ctrl     *conn            // control connection used for metadata requests
	metadata *meta.Response   // last metadata response seen by the pool
	err      error            // last error from metadata requests
	layout   protocol.Cluster // cluster layout built from metadata response
}

func (p *connPool) closeIdleConnections() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, conn := range p.conns {
		conn.unref()
	}

	for id := range p.conns {
		delete(p.conns, id)
	}

	if p.done != nil {
		p.done.trigger()
		p.ctrl = nil
		p.done = nil
	}
}

func (p *connPool) cachedMetadata() (*meta.Response, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.metadata, p.err
}

func (p *connPool) roundTrip(ctx context.Context, req protocol.Message) (protocol.Message, error) {
	canceled := ctx.Done()
	// This first select should never block after the first metadata response
	// that would mark the pool as `ready`.
	select {
	case <-p.ready:
	case <-canceled:
		return nil, ctx.Err()
	}

	var refreshMetadata bool
	switch m := req.(type) {
	case *meta.Request:
		// We serve metadata requests directly from the transport cache.
		//
		// This reduces the number of round trips to kafka brokers while keeping
		// the logic simple when applying partitioning strategies.
		r, err := p.cachedMetadata()
		if err != nil {
			return nil, err
		}
		return filterMetadataResponse(m, r), nil
	case *createtopics.Request, *deletetopics.Request:
		// Force an update of the metadata when adding or removing topics,
		// otherwise the cached state would get out of sync.
		refreshMetadata = true
	}

	c, err := p.grabConn(req)
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

	var r connResponse
	select {
	case r = <-res:
	case <-canceled:
		return nil, ctx.Err()
	}
	if r.err != nil {
		return nil, r.err
	}

	if refreshMetadata {
		notify := make(event)
		select {
		case <-canceled:
		case p.wake <- notify:
			select {
			case <-notify:
			case <-canceled:
			}
		}
	}

	return r.res, nil
}

func (p *connPool) setReady() {
	p.once.Do(p.ready.trigger)
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
func (p *connPool) grabConn(req protocol.Message) (*conn, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	var c *conn
	switch m := req.(type) {
	case protocol.BrokerMessage:
		layout, err := p.layout, p.err
		if err != nil {
			return nil, err
		}
		broker, err := m.Broker(layout)
		if err != nil {
			return nil, err
		}
		c = p.conns[broker.ID]
	default:
		c = p.ctrl
	}

	c.ref()
	return c, nil
}

// update is called periodically by the goroutine running the discover method
// to refresh the cluster layout information used by the transport to route
// requests to brokers.
func (p *connPool) update(metadata *meta.Response, err error) {
	var layout protocol.Cluster

	if metadata != nil {
		metadata.ThrottleTimeMs = 0

		// Normalize the lists so we can apply binary search on them.
		sortMetadataBrokers(metadata.Brokers)
		sortMetadataTopics(metadata.Topics)

		for i := range metadata.Topics {
			t := &metadata.Topics[i]
			sortMetadataPartitions(t.Partitions)
		}

		layout = makeLayout(metadata)
	}

	p.mutex.Lock()
	defer p.setReady()
	defer p.mutex.Unlock()

	if err != nil {
		// Only update the error on the transport if the cluster layout was
		// unknown. This ensures that we prioritize a previously known state
		// of the cluster to reduce the impact of transient failures.
		if p.metadata == nil {
			p.err = err
		}
		return
	}

	// We got a successful update, we will be using this state from now on,
	// and won't report errors seen trying to refresh the cluster layout
	// state. If the state gets out of sync, some requests may fail
	// (e.g. Produce requests going to brokers that are not leading a
	// partition), the program is expected to handle these errors and retry
	// accordingly.
	p.metadata, p.layout, p.err = metadata, layout, nil

	for id, broker := range p.layout.Brokers {
		if _, ok := p.conns[id]; !ok {
			p.conns[id] = p.connect(broker.Network(), broker.String())
		}
	}

	for id, conn := range p.conns {
		if _, ok := p.layout.Brokers[id]; !ok {
			conn.unref()
			delete(p.conns, id)
		}
	}
}

// discover is the entry point of an internal goroutine for the transport which
// periodically requests updates of the cluster metadata and refreshes the
// transport cached cluster layout.
func (p *connPool) discover(conn *conn, wake <-chan event, done <-chan struct{}) {
	defer conn.unref()

	prng := rand.New(rand.NewSource(time.Now().UnixNano()))
	metadataTTL := func() time.Duration {
		return time.Duration(prng.Int63n(int64(p.metadataTTL)))
	}

	timer := time.NewTimer(metadataTTL())
	defer timer.Stop()

	res := make(chan connResponse, 1)
	req := &meta.Request{
		IncludeCustomerAuthorizedOperations: true,
		IncludeTopicAuthorizedOperations:    true,
	}

	var notify event
	for {
		select {
		case conn.reqs <- connRequest{
			req: req,
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

		res, _ := r.res.(*meta.Response)
		p.update(res, r.err)

		if notify != nil {
			notify.trigger()
			notify = nil
		}

		select {
		case <-timer.C:
			timer.Reset(metadataTTL())
		case <-done:
			return
		case notify = <-wake:
		}
	}
}

func filterMetadataResponse(req *meta.Request, res *meta.Response) *meta.Response {
	ret := *res

	if len(req.TopicNames) != 0 {
		ret.Topics = make([]meta.ResponseTopic, len(req.TopicNames))

		for i, topicName := range req.TopicNames {
			j, ok := findMetadataTopic(res.Topics, topicName)
			if ok {
				ret.Topics[i] = res.Topics[j]
			} else {
				ret.Topics[i] = meta.ResponseTopic{
					ErrorCode: int16(UnknownTopicOrPartition),
					Name:      topicName,
				}
			}
		}
	}

	return &ret
}

func findMetadataTopic(topics []meta.ResponseTopic, topicName string) (int, bool) {
	i := sort.Search(len(topics), func(i int) bool {
		return topics[i].Name >= topicName
	})
	return i, i >= 0 && i < len(topics) && topics[i].Name == topicName
}

func sortMetadataBrokers(brokers []meta.ResponseBroker) {
	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].NodeID < brokers[j].NodeID
	})
}

func sortMetadataTopics(topics []meta.ResponseTopic) {
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})
}

func sortMetadataPartitions(partitions []meta.ResponsePartition) {
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].PartitionIndex < partitions[j].PartitionIndex
	})
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

func (p *connPool) connect(network, address string) *conn {
	reqs := make(chan connRequest)
	conn := &conn{
		refc:     1,
		pool:     p,
		reqs:     reqs,
		recycled: make(map[int32]struct{}),
		inflight: make(map[int32]connRequest),
	}
	go conn.run(reqs)
	return conn
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
		Timeout:   3 * time.Second,
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
	pool *connPool
	reqs chan<- connRequest
	// Shared state of the connection, this is synchronized on the mutex through
	// calls to the synchronized method. Both goroutines of the connection share
	// the state maintained in these fields.
	mutex    sync.Mutex
	idgen    int32                 // generates new correlation ids
	recycled map[int32]struct{}    // correlation ids available for reuse
	inflight map[int32]connRequest // set of inflight requests on the connection
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

type debug string

func (d debug) Write(b []byte) (int, error) {
	fmt.Printf("%s\n%s", d, hex.Dump(b))
	return len(b), nil
}

func (c *conn) connect() (net.Conn, *bufio.ReadWriter, map[protocol.ApiKey]int16, error) {
	deadline := time.Now().Add(c.pool.dialTimeout)

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	netConn, err := c.pool.dial(ctx, c.pool.network, c.pool.address)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() {
		if netConn != nil {
			netConn.Close()
		}
	}()

	if c.pool.tls != nil {
		netConn = tls.Client(netConn, c.pool.tls)
	}

	nc := netConn
	rw := &bufio.ReadWriter{
		Reader: bufio.NewReaderSize(io.TeeReader(netConn, debug("<")), bufferSize),
		Writer: bufio.NewWriterSize(io.MultiWriter(netConn, debug(">")), bufferSize),
	}

	if err := nc.SetDeadline(deadline); err != nil {
		return nil, nil, nil, err
	}

	if c.pool.sasl != nil {
		if err := c.authenticateSASL(ctx, rw, c.pool.sasl); err != nil {
			return nil, nil, nil, err
		}
	}

	r, err := protocol.RoundTrip(rw, v0, c.pool.clientID, new(apiversions.Request))
	if err != nil {
		return nil, nil, nil, err
	}
	res := r.(*apiversions.Response)
	ver := make(map[protocol.ApiKey]int16, len(res.ApiKeys))

	if res.ErrorCode != 0 {
		return nil, nil, nil, fmt.Errorf("negotating API versions with kafka broker at %s: %w", c.pool.address, Error(res.ErrorCode))
	}

	for _, r := range res.ApiKeys {
		apiKey := protocol.ApiKey(r.ApiKey)
		ver[apiKey] = apiKey.SelectVersion(r.MinVersion, r.MaxVersion)
	}

	if err := nc.SetDeadline(time.Now().Add(c.pool.idleTimeout)); err != nil {
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
				conn.SetDeadline(time.Now().Add(c.pool.idleTimeout))
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
			conn.SetDeadline(time.Now().Add(c.pool.readTimeout))

			if err := protocol.WriteRequest(wb, r.version, id, c.pool.clientID, r.req); err != nil {
				fmt.Println("error writing request:", err)
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
	msg, err := protocol.RoundTrip(rw, v0, c.pool.clientID, &saslhandshake.Request{
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
	msg, err := protocol.RoundTrip(rw, v1, c.pool.clientID, &saslauthenticate.Request{
		AuthBytes: data,
	})
	if err != nil {
		return nil, err
	}
	res := msg.(*saslauthenticate.Response)
	if res.ErrorCode != 0 {
		err = makeError(res.ErrorCode, res.ErrorMessage)
	}
	return res.AuthBytes, err
}

var (
	_ RoundTripper = (*Transport)(nil)
)
