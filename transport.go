package kafka

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
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
	"github.com/segmentio/kafka-go/protocol/findcoordinator"
	meta "github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/segmentio/kafka-go/protocol/saslauthenticate"
	"github.com/segmentio/kafka-go/protocol/saslhandshake"
	"github.com/segmentio/kafka-go/sasl"
)

// Request is an interface implemented by types that represent messages sent
// from kafka clients to brokers.
type Request = protocol.Message

// Response is an interface implemented by types that represent messages sent
// from kafka brokers in response to client requests.
type Response = protocol.Message

// RoundTripper is an interface implemented by types which support interacting
// with kafka brokers.
type RoundTripper interface {
	// RoundTrip sends a request to a kafka broker and returns the response that
	// was received, or a non-nil error.
	//
	// The context passed as first argument can be used to asynchronnously abort
	// the call if needed.
	RoundTrip(context.Context, net.Addr, Request) (Response, error)
}

// Transport is an implementation of the RoundTripper interface.
//
// Transport values manage a pool of connections and automatically discovers the
// clusters layout to route requests to the appropriate brokers.
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
		pool.unref()
	}

	for k := range t.pools {
		delete(t.pools, k)
	}
}

// RoundTrip sends a request to a kafka cluster and returns the response, or an
// error if no responses were received.
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
//	if err != nil {
//		...
//	} else {
//		res := r.(*fetch.Response)
//		...
//	}
//
// The transport automatically selects the highest version of the API that is
// supported by both the kafka-go package and the kafka broker. The negotiation
// happens transparently once when connections are established.
//
// This API was introduced in version 0.4 as a way to leverage the lower-level
// features of the kafka protocol, but also provide a more efficient way of
// managing connections to kafka brokers.
func (t *Transport) RoundTrip(ctx context.Context, addr net.Addr, req Request) (Response, error) {
	p := t.grabPool(addr)
	defer p.unref()
	return p.roundTrip(ctx, req)
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
	if p != nil {
		p.ref()
	}
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
		p.ref()
		return p
	}

	ctx, cancel := context.WithCancel(context.Background())

	p = &connPool{
		refc: 2,

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

		ready:  make(event),
		wake:   make(chan event),
		conns:  make(map[int]*conn),
		cancel: cancel,
	}

	p.ctrl = p.connect(ctx, k.network, k.address, -1)
	go p.discover(ctx, p.ctrl, p.wake)

	if t.pools == nil {
		t.pools = make(map[networkAddress]*connPool)
	}
	t.pools[k] = p
	return p
}

type event chan struct{}

func (e event) trigger() { close(e) }

type connPool struct {
	refc uintptr
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
	once   sync.Once  // ensure that `ready` is triggered only once
	ready  event      // triggered after the first metadata update
	wake   chan event // used to force metadata updates
	cancel context.CancelFunc
	// Mutable fields of the connection pool, access must be synchronized.
	mutex sync.RWMutex
	conns map[int]*conn // data connections used for produce/fetch/etc...
	ctrl  *conn         // control connection used for metadata requests
	state atomic.Value  // cached cluster state
}

type connPoolState struct {
	metadata *meta.Response   // last metadata response seen by the pool
	err      error            // last error from metadata requests
	layout   protocol.Cluster // cluster layout built from metadata response
}

func (p *connPool) grabState() connPoolState {
	state, _ := p.state.Load().(connPoolState)
	return state
}

func (p *connPool) setState(state connPoolState) {
	p.state.Store(state)
}

func (p *connPool) ref() {
	atomic.AddUintptr(&p.refc, +1)
}

func (p *connPool) unref() {
	if atomic.AddUintptr(&p.refc, ^uintptr(0)) == 0 {
		p.cancel()
	}
}

func (p *connPool) roundTrip(ctx context.Context, req Request) (Response, error) {
	cancel := ctx.Done()
	// This first select should never block after the first metadata response
	// that would mark the pool as `ready`.
	select {
	case <-p.ready:
	case <-cancel:
		return nil, ctx.Err()
	}

	var refreshMetadata bool
	defer func() {
		if refreshMetadata && ctx.Err() == nil {
			p.refreshMetadata(ctx)
		}
	}()

	p.mutex.RLock()
	defer p.mutex.RUnlock()

	var state = p.grabState()
	var response promise

	switch m := req.(type) {
	case *meta.Request:
		// We serve metadata requests directly from the transport cache.
		//
		// This reduces the number of round trips to kafka brokers while keeping
		// the logic simple when applying partitioning strategies.
		return filterMetadataResponse(m, state.metadata), nil

	case *createtopics.Request, *deletetopics.Request:
		// Force an update of the metadata when adding or removing topics,
		// otherwise the cached state would get out of sync.
		refreshMetadata = true

	case protocol.Mapper:
		// Messages that implement the Mapper interface trigger the creation of
		// multiple requests that are all merged back into a single results by
		// a reducer.
		messages, reducer, err := m.Map(state.layout)
		if err != nil {
			return nil, err
		}
		promises := make([]promise, len(messages))
		for i, m := range messages {
			promises[i] = p.sendRequest(ctx, m, state)
		}
		response = join(promises, messages, reducer)
	}

	if response == nil {
		response = p.sendRequest(ctx, req, state)
	}

	return response.await(ctx)
}

func (p *connPool) refreshMetadata(ctx context.Context) {
	cancel := ctx.Done()
	notify := make(event)
	select {
	case <-cancel:
	case p.wake <- notify:
		select {
		case <-notify:
		case <-cancel:
		}
	}
}

func (p *connPool) setReady() {
	p.once.Do(p.ready.trigger)
}

// update is called periodically by the goroutine running the discover method
// to refresh the cluster layout information used by the transport to route
// requests to brokers.
func (p *connPool) update(ctx context.Context, metadata *meta.Response, err error) {
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

	state := p.grabState()
	addBrokers := make(map[int]struct{})
	delBrokers := make(map[int]struct{})

	for id := range layout.Brokers {
		if _, ok := state.layout.Brokers[id]; !ok {
			addBrokers[id] = struct{}{}
		}
	}

	for id := range state.layout.Brokers {
		if _, ok := layout.Brokers[id]; !ok {
			delBrokers[id] = struct{}{}
		}
	}

	if err != nil {
		// Only update the error on the transport if the cluster layout was
		// unknown. This ensures that we prioritize a previously known state
		// of the cluster to reduce the impact of transient failures.
		if state.metadata == nil {
			state.err = err
		}
	} else {
		state.metadata, state.layout = metadata, layout
	}

	p.setState(state)
	p.setReady()

	if len(addBrokers) != 0 || len(delBrokers) != 0 {
		// Only acquire the lock when there is a change of layout. This is an
		// infrequent event so we don't risk introducing regular contention on
		// the mutex if we were to lock it on every update.
		p.mutex.Lock()
		defer p.mutex.Unlock()

		if ctx.Err() != nil {
			return // the pool has been closed, no need to update
		}

		for id := range addBrokers {
			broker := layout.Brokers[id]
			p.conns[id] = p.connect(ctx, broker.Network(), broker.String(), id)
		}

		for id := range delBrokers {
			conn := p.conns[id]
			close(conn.reqs)
			delete(p.conns, id)
		}
	}
}

// discover is the entry point of an internal goroutine for the transport which
// periodically requests updates of the cluster metadata and refreshes the
// transport cached cluster layout.
func (p *connPool) discover(ctx context.Context, conn *conn, wake <-chan event) {
	prng := rand.New(rand.NewSource(time.Now().UnixNano()))
	metadataTTL := func() time.Duration {
		return time.Duration(prng.Int63n(int64(p.metadataTTL)))
	}

	timer := time.NewTimer(metadataTTL())
	defer timer.Stop()

	res := make(async, 1)
	req := &meta.Request{
		IncludeClusterAuthorizedOperations: true,
		IncludeTopicAuthorizedOperations:   true,
	}

	var notify event
	var done = ctx.Done()
	for {
		select {
		case conn.reqs <- connRequest{
			req: req,
			res: res,
		}:
		case <-done:
			return
		}

		r, err := res.await(ctx)
		if err != nil && err == ctx.Err() {
			return
		}

		res, _ := r.(*meta.Response)
		p.update(ctx, res, err)

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

// grabBrokerConn returns a connection to a specific broker represented by the
// broker id passed as argument. If the broker id was not known, an error is
// returned.
func (p *connPool) grabBrokerConn(brokerID int) (*conn, error) {
	c := p.conns[brokerID]
	if c == nil {
		return nil, BrokerNotAvailable
	}
	return c, nil
}

// grabClusterConn returns the connection to the kafka cluster that the pool is
// configured to connect to.
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
func (p *connPool) grabClusterConn() (*conn, error) {
	return p.ctrl, nil
}

func (p *connPool) sendRequest(ctx context.Context, req Request, state connPoolState) promise {
	brokerID := -1

	switch m := req.(type) {
	case protocol.BrokerMessage:
		// Some requests are supposed to be sent to specific brokers (e.g. the
		// partition leaders). They implement the BrokerMessage interface to
		// delegate the routing decision to each message type.
		broker, err := m.Broker(state.layout)
		if err != nil {
			return reject(err)
		}
		brokerID = broker.ID

	case protocol.GroupMessage:
		// Some requests are supposed to be sent to a group coordinator,
		// look up which broker is currently the coordinator for the group
		// so we can get a connection to that broker.
		//
		// TODO: should we cache the coordinator info?
		p := p.sendRequest(ctx, &findcoordinator.Request{Key: m.Group()}, state)
		r, err := p.await(ctx)
		if err != nil {
			return reject(err)
		}
		brokerID = int(r.(*findcoordinator.Response).NodeID)
	}

	var c *conn
	var err error
	if brokerID >= 0 {
		c, err = p.grabBrokerConn(brokerID)
	} else {
		c, err = p.grabClusterConn()
	}
	if err != nil {
		return reject(err)
	}

	res := make(async, 1)

	select {
	case c.reqs <- connRequest{
		req: req,
		res: res,
	}:
	case <-ctx.Done():
		return reject(ctx.Err())
	}

	return res
}

func filterMetadataResponse(req *meta.Request, res *meta.Response) *meta.Response {
	ret := *res

	if req.TopicNames != nil {
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

func (p *connPool) connect(ctx context.Context, network, address string, broker int) *conn {
	reqs := make(chan connRequest)
	conn := &conn{
		broker:   broker,
		refc:     1,
		pool:     p,
		reqs:     reqs,
		inflight: make(map[int32]connRequest),
	}
	go conn.run(ctx, reqs)
	return conn
}

type connRequest struct {
	req Request
	res async
	// Lazily negotiated when writing the request, then reused to determine the
	// version of the message we're reading in the response.
	version int16
}

// The promise interface is used as a message passing abstraction to coordinate
// between goroutines that handle requests and responses.
type promise interface {
	// Waits until the promise is resolved, rejected, or the context canceled.
	await(context.Context) (Response, error)
}

// async is an implementation of the promise interface which supports resolving
// or rejecting the await call asynchronously.
type async chan interface{}

func (p async) await(ctx context.Context) (Response, error) {
	select {
	case x := <-p:
		switch v := x.(type) {
		case Response:
			return v, nil
		case error:
			return nil, v
		default:
			panic(fmt.Errorf("BUG: promise resolved with impossible value of type %T", v))
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p async) resolve(res Response) { p <- res }

func (p async) reject(err error) { p <- err }

// rejected is an implementation of the promise interface which is always
// returns an error. Values of this type are constructed using the reject
// function.
type rejected struct{ err error }

func reject(err error) promise { return &rejected{err: err} }

func (p *rejected) await(ctx context.Context) (Response, error) {
	return nil, p.err
}

// joined is an implementation of the promise interface which merges results
// from multiple promises into one await call using a reducer.
type joined struct {
	promises []promise
	requests []Request
	reducer  protocol.Reducer
}

func join(promises []promise, requests []Request, reducer protocol.Reducer) promise {
	return &joined{
		promises: promises,
		requests: requests,
		reducer:  reducer,
	}
}

func (p *joined) await(ctx context.Context) (Response, error) {
	results := make([]interface{}, len(p.promises))

	for i, p := range p.promises {
		m, err := p.await(ctx)
		if err != nil {
			results[i] = err
		} else {
			results[i] = m
		}
	}

	return p.reducer.Reduce(p.requests, results)
}

// Default dialer used by the transport connections when no Dial function
// was configured by the program.
var defaultDialer = net.Dialer{
	Timeout:   3 * time.Second,
	DualStack: true,
}

// conn represents a logical connection to a kafka broker. The actual network
// connections are lazily open before sending requests, and closed if they are
// unused for longer than the idle timeout.
type conn struct {
	broker int
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
	inflight map[int32]connRequest // set of inflight requests on the connection
}

func (c *conn) register(r connRequest) (id int32) {
	c.synchronized(func() {
		c.idgen++
		id = c.idgen
		c.inflight[id] = r
	})
	return
}

func (c *conn) unregister(id int32) (r connRequest, ok bool) {
	c.synchronized(func() {
		r, ok = c.inflight[id]
		if ok {
			delete(c.inflight, id)
		}
	})
	return
}

func (c *conn) cancel(err error) {
	c.synchronized(func() {
		for _, r := range c.inflight {
			r.res.reject(err)
		}
		for id := range c.inflight {
			delete(c.inflight, id)
		}
	})
}

func (c *conn) synchronized(f func()) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	f()
}

func (c *conn) connect() (*bufferedConn, map[protocol.ApiKey]int16, error) {
	deadline := time.Now().Add(c.pool.dialTimeout)

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	netConn, err := c.pool.dial(ctx, c.pool.network, c.pool.address)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if netConn != nil {
			netConn.Close()
		}
	}()

	if c.pool.tls != nil {
		netConn = tls.Client(netConn, c.pool.tls)
	}

	nc := newBufferedConn(netConn)

	if err := nc.SetDeadline(deadline); err != nil {
		return nil, nil, err
	}

	if c.pool.sasl != nil {
		if err := c.authenticateSASL(ctx, nc, c.pool.sasl); err != nil {
			return nil, nil, err
		}
	}

	r, err := protocol.RoundTrip(nc, v0, c.pool.clientID, new(apiversions.Request))
	if err != nil {
		return nil, nil, err
	}
	res := r.(*apiversions.Response)
	ver := make(map[protocol.ApiKey]int16, len(res.ApiKeys))

	if res.ErrorCode != 0 {
		return nil, nil, fmt.Errorf("negotating API versions with kafka broker at %s: %w", c.pool.address, Error(res.ErrorCode))
	}

	for _, r := range res.ApiKeys {
		apiKey := protocol.ApiKey(r.ApiKey)
		ver[apiKey] = apiKey.SelectVersion(r.MinVersion, r.MaxVersion)
	}

	if err := nc.SetDeadline(time.Now().Add(c.pool.idleTimeout)); err != nil {
		return nil, nil, err
	}

	netConn = nil
	return nc, ver, nil
}

func (c *conn) dispatch(conn *bufferedConn, done chan<- error) {
	for {
		_, id, err := conn.peekResponseSizeAndID()
		if err != nil {
			done <- dontExpectEOF(err)
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

		_, res, err := protocol.ReadResponse(conn, r.req.ApiKey(), r.version)
		if err != nil {
			r.res.reject(err)
			done <- err
			return
		}

		r.res.resolve(res)
		c.synchronized(func() {
			if len(c.inflight) == 0 {
				conn.SetDeadline(time.Now().Add(c.pool.idleTimeout))
			}
		})
	}
}

func (c *conn) run(ctx context.Context, reqs <-chan connRequest) {
	var conn *bufferedConn
	var done chan error
	var cancel = ctx.Done()
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
				var err error

				conn, versions, err = c.connect()
				if err != nil {
					r.res.reject(err)
					continue
				}

				done = make(chan error, 1)
				go c.dispatch(conn, done)
			}

			r.version = versions[r.req.ApiKey()]
			id := c.register(r)
			conn.SetDeadline(time.Now().Add(c.pool.readTimeout))

			if p, _ := r.req.(protocol.PreparedMessage); p != nil {
				p.Prepare(r.version)
			}

			switch err := protocol.WriteRequest(conn, r.version, id, c.pool.clientID, r.req); {
			case err == nil:
			case errors.Is(err, protocol.ErrNoRecords):
				c.unregister(id)
				r.res.reject(err)
			default:
				closeConn(err)
			}

		case err := <-done:
			closeConn(err)

		case <-cancel:
			return
		}
	}
}

// authenticateSASL performs all of the required requests to authenticate this
// connection.  If any step fails, this function returns with an error.  A nil
// error indicates successful authentication.
//
// In case of error, this function *does not* close the connection.  That is the
// responsibility of the caller.
func (c *conn) authenticateSASL(ctx context.Context, rw io.ReadWriter, mechanism sasl.Mechanism) error {
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
func (c *conn) saslHandshake(rw io.ReadWriter, mechanism string) error {
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
func (c *conn) saslAuthenticate(rw io.ReadWriter, data []byte) ([]byte, error) {
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

// bufferedConn pairs a network connection and a read buffer. The protocol
// sub-package leverages the methods exposed by the bufio.Reader to optimize
// decoding of messages received on the connection.
type bufferedConn struct {
	*bufio.Reader
	net.Conn
}

func newBufferedConn(conn net.Conn) *bufferedConn {
	return &bufferedConn{
		Reader: bufio.NewReader(conn),
		Conn:   conn,
	}
}

func (c *bufferedConn) Read(b []byte) (int, error) {
	return c.Reader.Read(b)
}

func (c *bufferedConn) peekResponseSizeAndID() (int32, int32, error) {
	b, err := c.Peek(8)
	if err != nil {
		return 0, 0, err
	}
	size, id := makeInt32(b[:4]), makeInt32(b[4:])
	return size, id, nil
}

var (
	_ RoundTripper = (*Transport)(nil)
)
