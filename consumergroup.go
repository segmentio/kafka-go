package kafka

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// ErrGroupClosed is returned by ConsumerGroup.Next when the group has already
// been closed.
var ErrGroupClosed = errors.New("consumer group is closed")

// ErrGenerationEnded is returned by the context.Context issued by the
// Generation's Start function when the context has been closed.
var ErrGenerationEnded = errors.New("consumer group generation has ended")

const (
	// defaultProtocolType holds the default protocol type documented in the
	// kafka protocol
	//
	// See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-GroupMembershipAPI
	defaultProtocolType = "consumer"

	// defaultHeartbeatInterval contains the default time between heartbeats.  If
	// the coordinator does not receive a heartbeat within the session timeout interval,
	// the consumer will be considered dead and the coordinator will rebalance the
	// group.
	//
	// As a rule, the heartbeat interval should be no greater than 1/3 the session timeout.
	defaultHeartbeatInterval = 3 * time.Second

	// defaultSessionTimeout contains the default interval the coordinator will wait
	// for a heartbeat before marking a consumer as dead.
	defaultSessionTimeout = 30 * time.Second

	// defaultRebalanceTimeout contains the amount of time the coordinator will wait
	// for consumers to issue a join group once a rebalance has been requested.
	defaultRebalanceTimeout = 30 * time.Second

	// defaultJoinGroupBackoff is the amount of time to wait after a failed
	// consumer group generation before attempting to re-join.
	defaultJoinGroupBackoff = 5 * time.Second

	// defaultRetentionTime holds the length of time a the consumer group will be
	// saved by kafka.  This value tells the broker to use its configured value.
	defaultRetentionTime = -1 * time.Millisecond

	// defaultPartitionWatchTime contains the amount of time the kafka-go will wait to
	// query the brokers looking for partition changes.
	defaultPartitionWatchTime = 5 * time.Second

	// defaultTimeout is the deadline to set when interacting with the
	// consumer group coordinator.
	defaultTimeout = 5 * time.Second
)

// ConsumerGroupConfig is a configuration object used to create new instances of
// ConsumerGroup.
type ConsumerGroupConfig struct {
	// ID is the consumer group ID.  It must not be empty.
	ID string

	// The list of broker addresses used to connect to the kafka cluster.  It
	// must not be empty.
	Brokers []string

	// Topics is the list of topics that will be consumed by this group.  It
	// will usually have a single value, but it is permitted to have multiple
	// for more complex use cases.
	Topics []string

	// A transport used to send messages to kafka clusters.
	//
	// If nil, DefaultTransport will be used.
	//
	// Default: DefaultTransport
	Transport RoundTripper

	// GroupBalancers is the priority-ordered list of client-side consumer group
	// balancing strategies that will be offered to the coordinator.  The first
	// strategy that all group members support will be chosen by the leader.
	//
	// Default: [Range, RoundRobin]
	GroupBalancers []GroupBalancer

	// HeartbeatInterval sets the optional frequency at which the reader sends the consumer
	// group heartbeat update.
	//
	// Default: 3s
	HeartbeatInterval time.Duration

	// PartitionWatchInterval indicates how often a reader checks for partition changes.
	// If a reader sees a partition change (such as a partition add) it will rebalance the group
	// picking up new partitions.
	//
	// Default: 5s
	PartitionWatchInterval time.Duration

	// WatchForPartitionChanges is used to inform kafka-go that a consumer group should be
	// polling the brokers and rebalancing if any partition changes happen to the topic.
	WatchPartitionChanges bool

	// SessionTimeout optionally sets the length of time that may pass without a heartbeat
	// before the coordinator considers the consumer dead and initiates a rebalance.
	//
	// Default: 30s
	SessionTimeout time.Duration

	// RebalanceTimeout optionally sets the length of time the coordinator will wait
	// for members to join as part of a rebalance.  For kafka servers under higher
	// load, it may be useful to set this value higher.
	//
	// Default: 30s
	RebalanceTimeout time.Duration

	// JoinGroupBackoff optionally sets the length of time to wait before re-joining
	// the consumer group after an error.
	//
	// Default: 5s
	JoinGroupBackoff time.Duration

	// RetentionTime optionally sets the length of time the consumer group will
	// be saved by the broker.  -1 will disable the setting and leave the
	// retention up to the broker's offsets.retention.minutes property.  By
	// default, that setting is 1 day for kafka < 2.0 and 7 days for kafka >=
	// 2.0.
	//
	// Default: -1
	RetentionTime time.Duration

	// StartOffset determines from whence the consumer group should begin
	// consuming when it finds a partition without a committed offset.  If
	// non-zero, it must be set to one of FirstOffset or LastOffset.
	//
	// Default: FirstOffset
	StartOffset int64

	// If not nil, specifies a logger used to report internal changes within the
	// reader.
	Logger Logger

	// ErrorLogger is the logger used to report errors. If nil, the reader falls
	// back to using Logger instead.
	ErrorLogger Logger

	// Timeout is the network timeout used when communicating with the consumer
	// group coordinator.  This value should not be too small since errors
	// communicating with the broker will generally cause a consumer group
	// rebalance, and it's undesirable that a transient network error intoduce
	// that overhead.  Similarly, it should not be too large or the consumer
	// group may be slow to respond to the coordinator failing over to another
	// broker.
	//
	// Default: 5s
	Timeout time.Duration

	// AllowAutoTopicCreation notifies writer to create topic if missing.
	AllowAutoTopicCreation bool

	// coord is used for mocking the coordinator in testing
	coord coordinator
}

// Validate method validates ConsumerGroupConfig properties and sets relevant
// defaults.
func (config *ConsumerGroupConfig) Validate() error {
	if len(config.Brokers) == 0 {
		return errors.New("cannot create a consumer group with an empty list of broker addresses")
	}

	if len(config.Topics) == 0 {
		return errors.New("cannot create a consumer group without a topic")
	}

	if config.ID == "" {
		return errors.New("cannot create a consumer group without an ID")
	}

	if config.Transport == nil {
		config.Transport = DefaultTransport
	}

	if len(config.GroupBalancers) == 0 {
		config.GroupBalancers = []GroupBalancer{
			RangeGroupBalancer{},
			RoundRobinGroupBalancer{},
		}
	}

	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = defaultHeartbeatInterval
	}

	if config.SessionTimeout == 0 {
		config.SessionTimeout = defaultSessionTimeout
	}

	if config.PartitionWatchInterval == 0 {
		config.PartitionWatchInterval = defaultPartitionWatchTime
	}

	if config.RebalanceTimeout == 0 {
		config.RebalanceTimeout = defaultRebalanceTimeout
	}

	if config.JoinGroupBackoff == 0 {
		config.JoinGroupBackoff = defaultJoinGroupBackoff
	}

	if config.RetentionTime == 0 {
		config.RetentionTime = defaultRetentionTime
	}

	if config.HeartbeatInterval < 0 || (config.HeartbeatInterval/time.Millisecond) >= math.MaxInt32 {
		return fmt.Errorf("HeartbeatInterval out of bounds: %d", config.HeartbeatInterval)
	}

	if config.SessionTimeout < 0 || (config.SessionTimeout/time.Millisecond) >= math.MaxInt32 {
		return fmt.Errorf("SessionTimeout out of bounds: %d", config.SessionTimeout)
	}

	if config.RebalanceTimeout < 0 || (config.RebalanceTimeout/time.Millisecond) >= math.MaxInt32 {
		return fmt.Errorf("RebalanceTimeout out of bounds: %d", config.RebalanceTimeout)
	}

	if config.JoinGroupBackoff < 0 || (config.JoinGroupBackoff/time.Millisecond) >= math.MaxInt32 {
		return fmt.Errorf("JoinGroupBackoff out of bounds: %d", config.JoinGroupBackoff)
	}

	if config.RetentionTime < 0 && config.RetentionTime != defaultRetentionTime {
		return fmt.Errorf("RetentionTime out of bounds: %d", config.RetentionTime)
	}

	if config.PartitionWatchInterval < 0 || (config.PartitionWatchInterval/time.Millisecond) >= math.MaxInt32 {
		return fmt.Errorf("PartitionWachInterval out of bounds %d", config.PartitionWatchInterval)
	}

	if config.StartOffset == 0 {
		config.StartOffset = FirstOffset
	}

	if config.StartOffset != FirstOffset && config.StartOffset != LastOffset {
		return fmt.Errorf("StartOffset is not valid %d", config.StartOffset)
	}

	if config.Timeout == 0 {
		config.Timeout = defaultTimeout
	}

	return nil
}

// PartitionAssignment represents the starting state of a partition that has
// been assigned to a consumer.
type PartitionAssignment struct {
	// ID is the partition ID.
	ID int

	// Offset is the initial offset at which this assignment begins.  It will
	// either be an absolute offset if one has previously been committed for
	// the consumer group or a relative offset such as FirstOffset when this
	// is the first time the partition have been assigned to a member of the
	// group.
	Offset int64
}

// genCtx adapts the done channel of the generation to a context.Context.  This
// is used by Generation.Start so that we can pass a context to go routines
// instead of passing around channels.
type genCtx struct {
	gen *Generation
}

func (c genCtx) Done() <-chan struct{} {
	return c.gen.done
}

func (c genCtx) Err() error {
	select {
	case <-c.gen.done:
		return ErrGenerationEnded
	default:
		return nil
	}
}

func (c genCtx) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (c genCtx) Value(interface{}) interface{} {
	return nil
}

// Generation represents a single consumer group generation.  The generation
// carries the topic+partition assignments for the given.  It also provides
// facilities for committing offsets and for running functions whose lifecycles
// are bound to the generation.
type Generation struct {
	// ID is the generation ID as assigned by the consumer group coordinator.
	ID int

	// GroupID is the name of the consumer group.
	GroupID string

	// MemberID is the ID assigned to this consumer by the consumer group
	// coordinator.
	MemberID string

	// Assignments is the initial state of this Generation.  The partition
	// assignments are grouped by topic.
	Assignments map[string][]PartitionAssignment

	// the following fields are used for process accounting to synchronize
	// between Start and close.  lock protects all of them.  done is closed
	// when the generation is ending in order to signal that the generation
	// should start self-desructing.  closed protects against double-closing
	// the done chan.  routines is a count of running go routines that have been
	// launched by Start.  joined will be closed by the last go routine to exit.
	lock     sync.Mutex
	done     chan struct{}
	closed   bool
	routines int
	joined   chan struct{}

	retentionMillis int64
	log             func(func(Logger))
	logError        func(func(Logger))

	coord coordinator
}

// close stops the generation and waits for all functions launched via Start to
// terminate.
func (g *Generation) close() {
	g.lock.Lock()
	if !g.closed {
		close(g.done)
		g.closed = true
	}
	// determine whether any go routines are running that we need to wait for.
	// waiting needs to happen outside of the critical section.
	r := g.routines
	g.lock.Unlock()

	// NOTE: r will be zero if no go routines were ever launched.  no need to
	// wait in that case.
	if r > 0 {
		<-g.joined
	}
}

// Start launches the provided function in a go routine and adds accounting such
// that when the function exits, it stops the current generation (if not
// already in the process of doing so).
//
// The provided function MUST support cancellation via the ctx argument and exit
// in a timely manner once the ctx is complete.  When the context is closed, the
// context's Error() function will return ErrGenerationEnded.
//
// When closing out a generation, the consumer group will wait for all functions
// launched by Start to exit before the group can move on and join the next
// generation.  If the function does not exit promptly, it will stop forward
// progress for this consumer and potentially cause consumer group membership
// churn.
func (g *Generation) Start(fn func(ctx context.Context)) {
	g.lock.Lock()
	defer g.lock.Unlock()

	// this is an edge case: if the generation has already closed, then it's
	// possible that the close func has already waited on outstanding go
	// routines and exited.
	//
	// nonetheless, it's important to honor that the fn is invoked in case the
	// calling function is waiting e.g. on a channel send or a WaitGroup.  in
	// such a case, fn should immediately exit because ctx.Err() will return
	// ErrGenerationEnded.
	if g.closed {
		go fn(genCtx{g})
		return
	}

	// register that there is one more go routine that's part of this gen.
	g.routines++

	go func() {
		fn(genCtx{g})
		g.lock.Lock()
		// shut down the generation as soon as one function exits.  this is
		// different from close() in that it doesn't wait for all go routines in
		// the generation to exit.
		if !g.closed {
			close(g.done)
			g.closed = true
		}
		g.routines--
		// if this was the last go routine in the generation, close the joined
		// chan so that close() can exit if it's waiting.
		if g.routines == 0 {
			close(g.joined)
		}
		g.lock.Unlock()
	}()
}

// CommitOffsets commits the provided topic+partition+offset combos to the
// consumer group coordinator.  This can be used to reset the consumer to
// explicit offsets.
func (g *Generation) CommitOffsets(offsets map[string]map[int]int64) error {
	if len(offsets) == 0 {
		return nil
	}

	topics := make(map[string][]OffsetCommit, len(offsets))
	for topic, partitions := range offsets {
		for p, o := range partitions {
			topics[topic] = append(topics[topic], OffsetCommit{
				Partition: p,
				Offset:    o,
			})
		}
	}

	request := &OffsetCommitRequest{
		GroupID:      g.GroupID,
		GenerationID: g.ID,
		MemberID:     g.MemberID,
		Topics:       topics,
	}

	_, err := g.coord.offsetCommit(genCtx{g}, request)
	if err == nil {
		// if logging is enabled, print out the partitions that were committed.
		g.log(func(l Logger) {
			var report []string
			for topic, offsets := range request.Topics {
				report = append(report, fmt.Sprintf("\ttopic: %s", topic))
				for _, p := range offsets {
					report = append(report, fmt.Sprintf("\t\tpartition %d: %d", p.Partition, p.Offset))
				}
			}
			l.Printf("committed offsets for group %s: \n%s", g.GroupID, strings.Join(report, "\n"))
		})
	}

	return err
}

// heartbeatLoop checks in with the consumer group coordinator at the provided
// interval.  It exits if it ever encounters an error, which would signal the
// end of the generation.
func (g *Generation) heartbeatLoop(interval time.Duration) {
	g.Start(func(ctx context.Context) {
		g.log(func(l Logger) {
			l.Printf("started heartbeat for group, %v [%v]", g.GroupID, interval)
		})
		defer g.log(func(l Logger) {
			l.Printf("stopped heartbeat for group %s\n", g.GroupID)
		})

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, err := g.coord.heartbeat(ctx, &HeartbeatRequest{
					GroupID:      g.GroupID,
					GenerationID: g.ID,
					MemberID:     g.MemberID,
				})
				if err != nil {
					return
				}
			}
		}
	})
}

// partitionWatcher queries kafka and watches for partition changes, triggering
// a rebalance if changes are found. Similar to heartbeat it's okay to return on
// error here as if you are unable to ask a broker for basic metadata you're in
// a bad spot and should rebalance. Commonly you will see an error here if there
// is a problem with the connection to the coordinator and a rebalance will
// establish a new connection to the coordinator.
func (g *Generation) partitionWatcher(interval time.Duration, topic string) {
	g.Start(func(ctx context.Context) {
		g.log(func(l Logger) {
			l.Printf("started partition watcher for group, %v, topic %v [%v]", g.GroupID, topic, interval)
		})
		defer g.log(func(l Logger) {
			l.Printf("stopped partition watcher for group, %v, topic %v", g.GroupID, topic)
		})

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		ops, err := g.coord.readPartitions(ctx, topic)
		if err != nil {
			g.logError(func(l Logger) {
				l.Printf("Problem getting partitions during startup, %v\n, Returning and setting up nextGeneration", err)
			})
			return
		}
		oParts := len(ops)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ops, err := g.coord.readPartitions(ctx, topic)
				switch {
				case err == nil, errors.Is(err, UnknownTopicOrPartition):
					if len(ops) != oParts {
						g.log(func(l Logger) {
							l.Printf("Partition changes found, reblancing group: %v.", g.GroupID)
						})
						return
					}

				default:
					g.logError(func(l Logger) {
						l.Printf("Problem getting partitions while checking for changes, %v", err)
					})
					var kafkaError Error
					if errors.As(err, &kafkaError) {
						continue
					}
					// other errors imply that we lost the connection to the coordinator, so we
					// should abort and reconnect.
					return
				}
			}
		}
	})
}

// coordinator is a subset of the functionality in Client in order to facilitate
// testing the consumer group...especially for error conditions that are
// difficult to instigate with a live broker running in docker.
type coordinator interface {
	joinGroup(context.Context, *JoinGroupRequest) (*JoinGroupResponse, error)
	syncGroup(context.Context, *SyncGroupRequest) (*SyncGroupResponse, error)
	leaveGroup(context.Context, *LeaveGroupRequest) (*LeaveGroupResponse, error)
	heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error)
	offsetFetch(context.Context, *OffsetFetchRequest) (*OffsetFetchResponse, error)
	offsetCommit(context.Context, *OffsetCommitRequest) (*OffsetCommitResponse, error)
	readPartitions(context.Context, ...string) ([]Partition, error)
}

// timeoutCoordinator wraps the Conn to ensure that every operation has a
// deadline.  Otherwise, it would be possible for requests to block indefinitely
// if the remote server never responds.  There are many spots where the consumer
// group needs to interact with the broker, so it feels less error prone to
// factor all of the deadline management into this shared location as opposed to
// peppering it all through where the code actually interacts with the broker.
type timeoutCoordinator struct {
	timeout          time.Duration
	sessionTimeout   time.Duration
	rebalanceTimeout time.Duration
	autoCreateTopic  bool
	client           *Client
}

func (t *timeoutCoordinator) joinGroup(ctx context.Context, req *JoinGroupRequest) (*JoinGroupResponse, error) {
	// in the case of join group, the consumer group coordinator may wait up
	// to rebalance timeout in order to wait for all members to join.
	ctx, cancel := context.WithTimeout(ctx, t.timeout+t.rebalanceTimeout)
	defer cancel()
	return t.client.JoinGroup(ctx, req)
}

func (t *timeoutCoordinator) syncGroup(ctx context.Context, req *SyncGroupRequest) (*SyncGroupResponse, error) {
	// in the case of sync group, the consumer group leader is given up to
	// the session timeout to respond before the coordinator will give up.
	ctx, cancel := context.WithTimeout(ctx, t.timeout+t.sessionTimeout)
	defer cancel()
	return t.client.SyncGroup(ctx, req)
}

func (t *timeoutCoordinator) leaveGroup(ctx context.Context, req *LeaveGroupRequest) (*LeaveGroupResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	return t.client.LeaveGroup(ctx, req)
}

func (t *timeoutCoordinator) heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	return t.client.Heartbeat(ctx, req)
}

func (t *timeoutCoordinator) offsetFetch(ctx context.Context, req *OffsetFetchRequest) (*OffsetFetchResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	return t.client.OffsetFetch(ctx, req)
}

func (t *timeoutCoordinator) offsetCommit(ctx context.Context, req *OffsetCommitRequest) (*OffsetCommitResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	return t.client.OffsetCommit(ctx, req)
}

func (t *timeoutCoordinator) readPartitions(ctx context.Context, topics ...string) ([]Partition, error) {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	metaResp, err := t.client.Metadata(ctx, &MetadataRequest{
		Topics:                 topics,
		AllowAutoTopicCreation: t.autoCreateTopic,
	})
	if err != nil {
		return nil, err
	}

	var partitions []Partition

	for _, topic := range metaResp.Topics {
		if topic.Error != nil {
			return nil, topic.Error
		}
		partitions = append(partitions, topic.Partitions...)
	}
	return partitions, nil
}

// NewConsumerGroup creates a new ConsumerGroup.  It returns an error if the
// provided configuration is invalid.  It does not attempt to connect to the
// Kafka cluster.  That happens asynchronously, and any errors will be reported
// by Next.
func NewConsumerGroup(config ConsumerGroupConfig) (*ConsumerGroup, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	coord := config.coord
	if coord == nil {
		coord = &timeoutCoordinator{
			timeout:          config.Timeout,
			sessionTimeout:   config.SessionTimeout,
			rebalanceTimeout: config.RebalanceTimeout,
			autoCreateTopic:  config.AllowAutoTopicCreation,
			client: &Client{
				Addr: TCP(config.Brokers...),
				// For some requests we send timeouts set to sums of the provided timeouts.
				// Set the abosolute timeout to be the sum of all timeouts to avoid timing out early.
				Timeout:   config.SessionTimeout + config.Timeout + config.RebalanceTimeout,
				Transport: config.Transport,
			},
		}
	}

	cg := &ConsumerGroup{
		config: config,
		coord:  coord,
		next:   make(chan *Generation),
		errs:   make(chan error),
	}
	cg.done, cg.close = context.WithCancel(context.Background())
	cg.wg.Add(1)
	go func() {
		cg.run()
		cg.wg.Done()
	}()
	return cg, nil
}

// ConsumerGroup models a Kafka consumer group.  A caller doesn't interact with
// the group directly.  Rather, they interact with a Generation.  Every time a
// member enters or exits the group, it results in a new Generation.  The
// Generation is where partition assignments and offset management occur.
// Callers will use Next to get a handle to the Generation.
type ConsumerGroup struct {
	config ConsumerGroupConfig
	coord  coordinator
	next   chan *Generation
	errs   chan error

	close context.CancelFunc
	done  context.Context
	wg    sync.WaitGroup
}

// Close terminates the current generation by causing this member to leave and
// releases all local resources used to participate in the consumer group.
// Close will also end the current generation if it is still active.
func (cg *ConsumerGroup) Close() error {
	cg.close()
	cg.wg.Wait()

	return nil
}

// Next waits for the next consumer group generation.  There will never be two
// active generations.  Next will never return a new generation until the
// previous one has completed.
//
// If there are errors setting up the next generation, they will be surfaced
// here.
//
// If the ConsumerGroup has been closed, then Next will return ErrGroupClosed.
func (cg *ConsumerGroup) Next(ctx context.Context) (*Generation, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-cg.done.Done():
		return nil, ErrGroupClosed
	case err := <-cg.errs:
		return nil, err
	case next := <-cg.next:
		return next, nil
	}
}

func (cg *ConsumerGroup) run() {
	// the memberID is the only piece of information that is maintained across
	// generations.  it starts empty and will be assigned on the first nextGeneration
	// when the joinGroup request is processed.  it may change again later if
	// the CG coordinator fails over or if the member is evicted.  otherwise, it
	// will be constant for the lifetime of this group.
	var memberID string
	var err error
	for {
		memberID, err = cg.nextGeneration(memberID)
		// backoff will be set if this go routine should sleep before continuing
		// to the next generation.  it will be non-nil in the case of an error
		// joining or syncing the group.
		var backoff <-chan time.Time

		switch {
		case err == nil:
			// no error...the previous generation finished normally.
			continue

		case errors.Is(err, ErrGroupClosed):
			// the CG has been closed...leave the group and exit loop.
			// use context.Background() here since cg.done is closed.
			_ = cg.leaveGroup(context.Background(), memberID)
			return
		case errors.Is(err, MemberIDRequired):
			// Some versions of Kafka will return MemberIDRequired as well
			// as the member ID to use. In this case we just want to retry
			// with the returned member ID.
			continue
		case errors.Is(err, RebalanceInProgress):
			// in case of a RebalanceInProgress, don't leave the group or
			// change the member ID, but report the error.  the next attempt
			// to join the group will then be subject to the rebalance
			// timeout, so the broker will be responsible for throttling
			// this loop.
		default:
			// leave the group and report the error if we had gotten far
			// enough so as to have a member ID.  also clear the member id
			// so we don't attempt to use it again.  in order to avoid
			// a tight error loop, backoff before the next attempt to join
			// the group.
			_ = cg.leaveGroup(cg.done, memberID)
			memberID = ""
			backoff = time.After(cg.config.JoinGroupBackoff)
		}
		// ensure that we exit cleanly in case the CG is done and no one is
		// waiting to receive on the unbuffered error channel.
		select {
		case <-cg.done.Done():
			return
		case cg.errs <- err:
		}
		// backoff if needed, being sure to exit cleanly if the CG is done.
		if backoff != nil {
			select {
			case <-cg.done.Done():
				// exit cleanly if the group is closed.
				return
			case <-backoff:
			}
		}
	}
}

func (cg *ConsumerGroup) nextGeneration(memberID string) (string, error) {
	var generationID int
	var groupAssignments GroupMemberAssignments
	var assignments map[string][]int
	var protocolName string
	var err error

	// join group.  this will join the group and prepare assignments if our
	// consumer is elected leader.  it may also change or assign the member ID.
	memberID, generationID, protocolName, groupAssignments, err = cg.joinGroup(memberID)
	if err != nil {
		cg.withErrorLogger(func(log Logger) {
			log.Printf("Failed to join group %s: %v", cg.config.ID, err)
		})
		return memberID, err
	}
	cg.withLogger(func(log Logger) {
		log.Printf("Joined group %s as member %s in generation %d", cg.config.ID, memberID, generationID)
	})

	// sync group
	assignments, err = cg.syncGroup(memberID, generationID, protocolName, groupAssignments)
	if err != nil {
		cg.withErrorLogger(func(log Logger) {
			log.Printf("Failed to sync group %s: %v", cg.config.ID, err)
		})
		return memberID, err
	}
	// fetch initial offsets.
	var offsets map[string]map[int]int64
	offsets, err = cg.fetchOffsets(assignments)
	if err != nil {
		cg.withErrorLogger(func(log Logger) {
			log.Printf("Failed to fetch offsets for group %s: %v", cg.config.ID, err)
		})
		return memberID, err
	}

	// create the generation.
	gen := Generation{
		ID:              generationID,
		GroupID:         cg.config.ID,
		MemberID:        memberID,
		Assignments:     cg.makeAssignments(assignments, offsets),
		coord:           cg.coord,
		done:            make(chan struct{}),
		joined:          make(chan struct{}),
		retentionMillis: int64(cg.config.RetentionTime / time.Millisecond),
		log:             cg.withLogger,
		logError:        cg.withErrorLogger,
	}

	// spawn all of the go routines required to facilitate this generation.  if
	// any of these functions exit, then the generation is determined to be
	// complete.
	gen.heartbeatLoop(cg.config.HeartbeatInterval)
	if cg.config.WatchPartitionChanges {
		for _, topic := range cg.config.Topics {
			gen.partitionWatcher(cg.config.PartitionWatchInterval, topic)
		}
	}

	// make this generation available for retrieval.  if the CG is closed before
	// we can send it on the channel, exit.  that case is required b/c the next
	// channel is unbuffered.  if the caller to Next has already bailed because
	// it's own teardown logic has been invoked, this would deadlock otherwise.
	select {
	case <-cg.done.Done():
		gen.close()
		return memberID, ErrGroupClosed // ErrGroupClosed will trigger leave logic.
	case cg.next <- &gen:
	}

	// wait for generation to complete.  if the CG is closed before the
	// generation is finished, exit and leave the group.
	select {
	case <-cg.done.Done():
		gen.close()
		return memberID, ErrGroupClosed // ErrGroupClosed will trigger leave logic.
	case <-gen.done:
		// time for next generation!  make sure all the current go routines exit
		// before continuing onward.
		gen.close()
		return memberID, nil
	}
}

// joinGroup attempts to join the reader to the consumer group.
// Returns GroupMemberAssignments is this Reader was selected as
// the leader.  Otherwise, GroupMemberAssignments will be nil.
//
// Possible kafka error codes returned:
//   - GroupLoadInProgress:
//   - GroupCoordinatorNotAvailable:
//   - NotCoordinatorForGroup:
//   - InconsistentGroupProtocol:
//   - InvalidSessionTimeout:
//   - GroupAuthorizationFailed:
func (cg *ConsumerGroup) joinGroup(memberID string) (string, int, string, GroupMemberAssignments, error) {
	request, err := cg.makeJoinGroupRequest(memberID)
	if err != nil {
		return "", 0, "", nil, err
	}

	response, err := cg.coord.joinGroup(cg.done, request)
	if err == nil && response.Error != nil {
		err = response.Error
	}
	if response != nil {
		memberID = response.MemberID
	}
	if err != nil {
		return memberID, 0, "", nil, err
	}

	generationID := response.GenerationID

	cg.withLogger(func(l Logger) {
		l.Printf("joined group %s as member %s in generation %d", cg.config.ID, memberID, generationID)
	})
	var assignments GroupMemberAssignments
	if iAmLeader := response.MemberID == response.LeaderID; iAmLeader {
		v, err := cg.assignTopicPartitions(response)
		if err != nil {
			return memberID, 0, "", nil, err
		}
		assignments = v

		cg.withLogger(func(l Logger) {
			for memberID, assignment := range assignments {
				for topic, partitions := range assignment {
					l.Printf("assigned member/topic/partitions %v/%v/%v", memberID, topic, partitions)
				}
			}
		})
	}

	cg.withLogger(func(l Logger) {
		l.Printf("joinGroup succeeded for response, %v.  generationID=%v, memberID=%v", cg.config.ID, response.GenerationID, response.MemberID)
	})

	return memberID, generationID, response.ProtocolName, assignments, nil
}

// makeJoinGroupRequest handles the logic of constructing a joinGroup
// request.
func (cg *ConsumerGroup) makeJoinGroupRequest(memberID string) (*JoinGroupRequest, error) {
	request := &JoinGroupRequest{
		GroupID:          cg.config.ID,
		MemberID:         memberID,
		SessionTimeout:   cg.config.SessionTimeout,
		RebalanceTimeout: cg.config.RebalanceTimeout,
		ProtocolType:     defaultProtocolType,
	}

	for _, balancer := range cg.config.GroupBalancers {
		userData, err := balancer.UserData()
		if err != nil {
			return nil, fmt.Errorf("unable to construct protocol metadata for member, %v: %w", balancer.ProtocolName(), err)
		}
		request.Protocols = append(request.Protocols, GroupProtocol{
			Name: balancer.ProtocolName(),
			Metadata: GroupProtocolSubscription{
				Topics:   cg.config.Topics,
				UserData: userData,
			},
		})
	}

	return request, nil
}

// makeMemberProtocolMetadata maps encoded member metadata ([]byte) into []GroupMember.
func (cg *ConsumerGroup) makeMemberProtocolMetadata(in []JoinGroupResponseMember) []GroupMember {
	members := make([]GroupMember, 0, len(in))
	for _, item := range in {
		members = append(members, GroupMember{
			ID:       item.ID,
			Topics:   item.Metadata.Topics,
			UserData: item.Metadata.UserData,
		})
	}
	return members
}

// assignTopicPartitions uses the selected GroupBalancer to assign members to
// their various partitions.
func (cg *ConsumerGroup) assignTopicPartitions(group *JoinGroupResponse) (GroupMemberAssignments, error) {
	cg.withLogger(func(l Logger) {
		l.Printf("selected as leader for group, %s\n", cg.config.ID)
	})
	balancer, ok := findGroupBalancer(group.ProtocolName, cg.config.GroupBalancers)
	if !ok {
		// NOTE : this shouldn't happen in practice...the broker should not
		//        return successfully from joinGroup unless all members support
		//        at least one common protocol.
		return nil, fmt.Errorf("unable to find selected balancer, %v, for group, %v", group.ProtocolName, cg.config.ID)
	}

	members := cg.makeMemberProtocolMetadata(group.Members)

	topics := extractTopics(members)
	partitions, err := cg.coord.readPartitions(cg.done, topics...)
	// it's not a failure if the topic doesn't exist yet.  it results in no
	// assignments for the topic.  this matches the behavior of the official
	// clients: java, python, and librdkafka.
	// a topic watcher can trigger a rebalance when the topic comes into being.
	if err != nil && !errors.Is(err, UnknownTopicOrPartition) {
		return nil, err
	}

	cg.withLogger(func(l Logger) {
		l.Printf("using '%v' balancer to assign group, %v", group.ProtocolName, cg.config.ID)
		for _, member := range group.Members {
			l.Printf("found member: %v/%#v", member.ID, member.Metadata.UserData)
		}
		for _, partition := range partitions {
			l.Printf("found topic/partition: %v/%v", partition.Topic, partition.ID)
		}
	})

	return balancer.AssignGroups(members, partitions), nil
}

// syncGroup completes the consumer group nextGeneration by accepting the
// memberAssignments (if this Reader is the leader) and returning this
// Readers subscriptions topic => partitions
//
// Possible kafka error codes returned:
//   - GroupCoordinatorNotAvailable:
//   - NotCoordinatorForGroup:
//   - IllegalGeneration:
//   - RebalanceInProgress:
//   - GroupAuthorizationFailed:
func (cg *ConsumerGroup) syncGroup(memberID string, generationID int, protocolName string, memberAssignments GroupMemberAssignments) (map[string][]int, error) {
	request := cg.makeSyncGroupRequest(memberID, generationID, protocolName, memberAssignments)
	response, err := cg.coord.syncGroup(cg.done, request)
	if err == nil && response.Error != nil {
		err = response.Error
	}
	if err != nil {
		return nil, err
	}
	if len(response.Assignment.AssignedPartitions) == 0 {
		cg.withLogger(func(l Logger) {
			l.Printf("received empty assignments for group, %v as member %s for generation %d", cg.config.ID, memberID, generationID)
		})
	}

	cg.withLogger(func(l Logger) {
		l.Printf("sync group finished for group, %v", cg.config.ID)
	})

	return response.Assignment.AssignedPartitions, nil
}

func (cg *ConsumerGroup) makeSyncGroupRequest(memberID string, generationID int, protocolName string, memberAssignments GroupMemberAssignments) *SyncGroupRequest {
	request := &SyncGroupRequest{
		GroupID:      cg.config.ID,
		GenerationID: generationID,
		MemberID:     memberID,
		ProtocolType: defaultProtocolType,
		ProtocolName: protocolName,
	}

	if memberAssignments != nil {
		request.Assignments = make([]SyncGroupRequestAssignment, 0, 1)

		for memberID, topics := range memberAssignments {
			topics32 := make(map[string][]int32)
			for topic, partitions := range topics {
				partitions32 := make([]int32, len(partitions))
				for i := range partitions {
					partitions32[i] = int32(partitions[i])
				}
				topics32[topic] = partitions32
			}
			request.Assignments = append(request.Assignments, SyncGroupRequestAssignment{
				MemberID: memberID,
				Assignment: GroupProtocolAssignment{
					AssignedPartitions: topics,
				},
			})
		}

		cg.withLogger(func(logger Logger) {
			logger.Printf("Syncing %d assignments for generation %d as member %s", len(request.Assignments), generationID, memberID)
		})
	}

	return request
}

func (cg *ConsumerGroup) fetchOffsets(subs map[string][]int) (map[string]map[int]int64, error) {
	req := &OffsetFetchRequest{
		GroupID: cg.config.ID,
		Topics:  subs,
	}

	offsets, err := cg.coord.offsetFetch(cg.done, req)
	if err != nil {
		return nil, err
	}

	offsetsByTopic := make(map[string]map[int]int64)
	for topic, offsets := range offsets.Topics {
		offsetsByPartition := map[int]int64{}
		for _, pr := range offsets {
			if pr.CommittedOffset < 0 {
				pr.CommittedOffset = cg.config.StartOffset
			}
			offsetsByPartition[pr.Partition] = pr.CommittedOffset
		}
		offsetsByTopic[topic] = offsetsByPartition
	}

	return offsetsByTopic, nil
}

func (cg *ConsumerGroup) makeAssignments(assignments map[string][]int, offsets map[string]map[int]int64) map[string][]PartitionAssignment {
	topicAssignments := make(map[string][]PartitionAssignment)
	for _, topic := range cg.config.Topics {
		topicPartitions := assignments[topic]
		topicAssignments[topic] = make([]PartitionAssignment, 0, len(topicPartitions))
		for _, partition := range topicPartitions {
			var offset int64
			partitionOffsets, ok := offsets[topic]
			if ok {
				offset, ok = partitionOffsets[partition]
			}
			if !ok {
				offset = cg.config.StartOffset
			}
			topicAssignments[topic] = append(topicAssignments[topic], PartitionAssignment{
				ID:     partition,
				Offset: offset,
			})
		}
	}
	return topicAssignments
}

// leaveGroup takes its ctx as an argument because when we close a CG
// we cancel sg.done so it will fail if we use that context.
func (cg *ConsumerGroup) leaveGroup(ctx context.Context, memberID string) error {
	// don't attempt to leave the group if no memberID was ever assigned.
	if memberID == "" {
		return nil
	}

	cg.withLogger(func(log Logger) {
		log.Printf("Leaving group %s, member %s", cg.config.ID, memberID)
	})

	_, err := cg.coord.leaveGroup(ctx, &LeaveGroupRequest{
		GroupID: cg.config.ID,
		Members: []LeaveGroupRequestMember{
			{
				ID: memberID,
			},
		},
	})
	if err != nil {
		cg.withErrorLogger(func(log Logger) {
			log.Printf("leave group failed for group, %v, and member, %v: %v", cg.config.ID, memberID, err)
		})
	}

	return err
}

func (cg *ConsumerGroup) withLogger(do func(Logger)) {
	if cg.config.Logger != nil {
		do(cg.config.Logger)
	}
}

func (cg *ConsumerGroup) withErrorLogger(do func(Logger)) {
	if cg.config.ErrorLogger != nil {
		do(cg.config.ErrorLogger)
	} else {
		cg.withLogger(do)
	}
}
