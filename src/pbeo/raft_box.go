package pbeo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"aegean/telemetry"
	raft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var errConsensusBoxStopped = errors.New("pbeo consensus box stopped")

type proposalRequest struct {
	entry  Entry
	result chan error
	span   trace.Span
}

type stepRequest struct {
	message raftpb.Message
}

type asyncLearnRequest struct {
	slot     uint64
	entry    Entry
	enqueued time.Time
}

type pendingAppendSignal struct {
	index     uint64
	requestID string
	span      trace.Span
}

type pendingCommitSignal struct {
	index uint64
	span  trace.Span
}

type drainReadyStats struct {
	readyBatches      int
	entries           int
	entryBytes        int
	committedEntries  int
	committedBytes    int
	messages          int
	messageBytes      int
	selfMessages      int
	sentMessages      int
	sentMessageBytes  int
	unreachablePeers  int
	confChanges       int
	normalEntries     int
	snapshots         int
	hardStates        int
	leaderTransitions int
	storageNanos      int64
	sendEnqueueNanos  int64
	selfStepNanos     int64
	learnDecodeNanos  int64
	learnCloneNanos   int64
	learnEnqueueNanos int64
	learnNanos        int64
	advanceNanos      int64
}

type raftConsensusBox struct {
	name       string
	selfID     uint64
	peerIDs    map[string]uint64
	peers      map[uint64]string
	send       SendRaftFunc
	sendBatch  SendRaftBatchFunc
	learn      LearnFunc
	learnBatch LearnBatchFunc

	leaderID    atomic.Uint64
	learnedSlot uint64

	proposals       chan proposalRequest
	prioritySteps   chan stepRequest
	backgroundSteps chan stepRequest
	sendQueues      map[uint64]chan raftpb.Message
	sendBatchSize   int
	learnBatchSize  int
	learnQueue      chan asyncLearnRequest
	unreachable     chan uint64
	spans           map[string]trace.Span
	commitWaitSpans map[string]trace.Span
	appendAckSpans  map[uint64][]pendingAppendSignal
	ackCommitSpans  map[uint64][]pendingCommitSignal
	stopOnce        sync.Once
	stopCh          chan struct{}
	doneCh          chan struct{}
}

const (
	raftProposalQueueSize     = 4096
	raftAsyncSendQueueSize    = 1024
	raftAsyncLearnQueueSize   = 4096
	defaultRaftSendBatchSize  = 64
	defaultRaftLearnBatchSize = 256
	raftLoopTraceWindow       = time.Second
)

type raftLoopTraceWindowStats struct {
	component      string
	nodeName       string
	selfID         uint64
	start          time.Time
	counts         map[string]int64
	durations      map[string]time.Duration
	durationCounts map[string]int64
	maxDurations   map[string]time.Duration
	lastGauges     map[string]int64
	maxGauges      map[string]int64
}

func newRaftLoopTraceWindowStats(component string, nodeName string, selfID uint64) *raftLoopTraceWindowStats {
	w := &raftLoopTraceWindowStats{
		component: component,
		nodeName:  nodeName,
		selfID:    selfID,
	}
	w.reset(time.Now())
	return w
}

func (w *raftLoopTraceWindowStats) reset(start time.Time) {
	w.start = start
	w.counts = make(map[string]int64)
	w.durations = make(map[string]time.Duration)
	w.durationCounts = make(map[string]int64)
	w.maxDurations = make(map[string]time.Duration)
	w.lastGauges = make(map[string]int64)
	w.maxGauges = make(map[string]int64)
}

func (w *raftLoopTraceWindowStats) addCount(name string, value int64) {
	if value == 0 {
		return
	}
	w.counts[name] += value
}

func (w *raftLoopTraceWindowStats) addDuration(name string, duration time.Duration) {
	if duration <= 0 {
		return
	}
	w.durations[name] += duration
	w.durationCounts[name]++
	if duration > w.maxDurations[name] {
		w.maxDurations[name] = duration
	}
}

func (w *raftLoopTraceWindowStats) observeGauge(name string, value int) {
	v := int64(value)
	w.lastGauges[name] = v
	if v > w.maxGauges[name] {
		w.maxGauges[name] = v
	}
}

func (w *raftLoopTraceWindowStats) recordQueues(proposals, prioritySteps, backgroundSteps, unreachable, learn int) {
	w.observeGauge("proposals_queued", proposals)
	w.observeGauge("priority_steps_queued", prioritySteps)
	w.observeGauge("background_steps_queued", backgroundSteps)
	w.observeGauge("unreachable_queued", unreachable)
	w.observeGauge("learn_queued", learn)
}

func (w *raftLoopTraceWindowStats) recordDrain(stats drainReadyStats, duration time.Duration) {
	w.addDuration("drain_ready", duration)
	w.addCount("ready_batches", int64(stats.readyBatches))
	w.addCount("ready_entries", int64(stats.entries))
	w.addCount("ready_committed_entries", int64(stats.committedEntries))
	w.addCount("ready_messages", int64(stats.messages))
	w.addCount("ready_self_messages", int64(stats.selfMessages))
	w.addCount("ready_sent_messages", int64(stats.sentMessages))
	w.addCount("ready_conf_changes", int64(stats.confChanges))
	w.addCount("ready_normal_entries", int64(stats.normalEntries))
	w.addCount("ready_snapshots", int64(stats.snapshots))
	w.addCount("ready_hard_states", int64(stats.hardStates))
	w.addCount("ready_leader_transitions", int64(stats.leaderTransitions))
	w.addDuration("storage", time.Duration(stats.storageNanos))
	w.addDuration("send_enqueue", time.Duration(stats.sendEnqueueNanos))
	w.addDuration("self_step", time.Duration(stats.selfStepNanos))
	w.addDuration("learn_decode", time.Duration(stats.learnDecodeNanos))
	w.addDuration("learn_clone", time.Duration(stats.learnCloneNanos))
	w.addDuration("learn_enqueue", time.Duration(stats.learnEnqueueNanos))
	w.addDuration("learn_committed", time.Duration(stats.learnNanos))
	w.addDuration("advance", time.Duration(stats.advanceNanos))
}

func (w *raftLoopTraceWindowStats) maybeFlush(leaderID uint64, reason string) {
	if time.Since(w.start) >= raftLoopTraceWindow {
		w.flush(leaderID, reason)
	}
}

func (w *raftLoopTraceWindowStats) flush(leaderID uint64, reason string) {
	if w.counts["iterations"] == 0 {
		w.reset(time.Now())
		return
	}
	now := time.Now()
	elapsed := now.Sub(w.start)
	attrs := []attribute.KeyValue{
		attribute.String("raft.loop.component", w.component),
		attribute.String("node.name", w.nodeName),
		attribute.Int64("raft.node_id", int64(w.selfID)),
		attribute.Int64("raft.leader_id", int64(leaderID)),
		attribute.Bool("raft.loop.is_leader", leaderID == w.selfID && w.selfID != 0),
		attribute.String("raft.loop.flush_reason", reason),
		attribute.Int64("raft.loop.window_us", elapsed.Microseconds()),
		attribute.Float64("raft.loop.iterations_per_sec", float64(w.counts["iterations"])/elapsed.Seconds()),
	}
	for _, key := range sortedInt64MapKeys(w.counts) {
		attrs = append(attrs, attribute.Int64("raft.loop.count."+key, w.counts[key]))
	}
	for _, key := range sortedDurationMapKeys(w.durations) {
		total := w.durations[key]
		count := w.durationCounts[key]
		avg := time.Duration(0)
		if count > 0 {
			avg = total / time.Duration(count)
		}
		attrs = append(attrs,
			attribute.Int64("raft.loop.duration."+key+".count", count),
			attribute.Int64("raft.loop.duration."+key+".total_us", total.Microseconds()),
			attribute.Int64("raft.loop.duration."+key+".avg_us", avg.Microseconds()),
			attribute.Int64("raft.loop.duration."+key+".max_us", w.maxDurations[key].Microseconds()),
		)
	}
	for _, key := range sortedInt64MapKeys(w.lastGauges) {
		attrs = append(attrs,
			attribute.Int64("raft.loop.gauge."+key+".last", w.lastGauges[key]),
			attribute.Int64("raft.loop.gauge."+key+".max", w.maxGauges[key]),
		)
	}

	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		w.component+".raft.loop.window",
		trace.WithTimestamp(w.start),
		trace.WithAttributes(attrs...),
	)
	span.End(trace.WithTimestamp(now))
	w.reset(now)
}

func sortedInt64MapKeys(values map[string]int64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedDurationMapKeys(values map[string]time.Duration) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

type raftLearnWorkerTraceWindowStats struct {
	nodeName       string
	selfID         uint64
	start          time.Time
	counts         map[string]int64
	durations      map[string]time.Duration
	durationCounts map[string]int64
	maxDurations   map[string]time.Duration
	lastGauges     map[string]int64
	maxGauges      map[string]int64
}

func newRaftLearnWorkerTraceWindowStats(nodeName string, selfID uint64) *raftLearnWorkerTraceWindowStats {
	w := &raftLearnWorkerTraceWindowStats{
		nodeName: nodeName,
		selfID:   selfID,
	}
	w.reset(time.Now())
	return w
}

func (w *raftLearnWorkerTraceWindowStats) reset(start time.Time) {
	w.start = start
	w.counts = make(map[string]int64)
	w.durations = make(map[string]time.Duration)
	w.durationCounts = make(map[string]int64)
	w.maxDurations = make(map[string]time.Duration)
	w.lastGauges = make(map[string]int64)
	w.maxGauges = make(map[string]int64)
}

func (w *raftLearnWorkerTraceWindowStats) addCount(name string, value int64) {
	if value == 0 {
		return
	}
	w.counts[name] += value
}

func (w *raftLearnWorkerTraceWindowStats) addDuration(name string, duration time.Duration) {
	if duration <= 0 {
		return
	}
	w.durations[name] += duration
	w.durationCounts[name]++
	if duration > w.maxDurations[name] {
		w.maxDurations[name] = duration
	}
}

func (w *raftLearnWorkerTraceWindowStats) observeGauge(name string, value int) {
	v := int64(value)
	w.lastGauges[name] = v
	if v > w.maxGauges[name] {
		w.maxGauges[name] = v
	}
}

func (w *raftLearnWorkerTraceWindowStats) recordBatch(batch []asyncLearnRequest, queueDepthBeforeDrain int, queueDepthAfterDrain int, callbackDuration time.Duration) {
	w.addCount("batches", 1)
	w.addCount("entries", int64(len(batch)))
	w.addDuration("callback", callbackDuration)
	now := time.Now()
	for _, request := range batch {
		w.addDuration("queue_wait", now.Sub(request.enqueued))
	}
	w.observeGauge("batch_size", len(batch))
	w.observeGauge("queue_depth_before_drain", queueDepthBeforeDrain)
	w.observeGauge("queue_depth_after_drain", queueDepthAfterDrain)
}

func (w *raftLearnWorkerTraceWindowStats) maybeFlush(leaderID uint64, reason string) {
	if time.Since(w.start) >= raftLoopTraceWindow {
		w.flush(leaderID, reason)
	}
}

func (w *raftLearnWorkerTraceWindowStats) flush(leaderID uint64, reason string) {
	if w.counts["batches"] == 0 {
		w.reset(time.Now())
		return
	}
	now := time.Now()
	elapsed := now.Sub(w.start)
	attrs := []attribute.KeyValue{
		attribute.String("raft.loop.component", "pbeo"),
		attribute.String("node.name", w.nodeName),
		attribute.Int64("raft.node_id", int64(w.selfID)),
		attribute.Int64("raft.leader_id", int64(leaderID)),
		attribute.Bool("raft.loop.is_leader", leaderID == w.selfID && w.selfID != 0),
		attribute.String("pbeo.raft.learn_worker.flush_reason", reason),
		attribute.Int64("pbeo.raft.learn_worker.window_us", elapsed.Microseconds()),
	}
	for _, key := range sortedInt64MapKeys(w.counts) {
		attrs = append(attrs, attribute.Int64("pbeo.raft.learn_worker.count."+key, w.counts[key]))
	}
	for _, key := range sortedDurationMapKeys(w.durations) {
		total := w.durations[key]
		count := w.durationCounts[key]
		avg := time.Duration(0)
		if count > 0 {
			avg = total / time.Duration(count)
		}
		attrs = append(attrs,
			attribute.Int64("pbeo.raft.learn_worker.duration."+key+".count", count),
			attribute.Int64("pbeo.raft.learn_worker.duration."+key+".total_us", total.Microseconds()),
			attribute.Int64("pbeo.raft.learn_worker.duration."+key+".avg_us", avg.Microseconds()),
			attribute.Int64("pbeo.raft.learn_worker.duration."+key+".max_us", w.maxDurations[key].Microseconds()),
		)
	}
	for _, key := range sortedInt64MapKeys(w.lastGauges) {
		attrs = append(attrs,
			attribute.Int64("pbeo.raft.learn_worker.gauge."+key+".last", w.lastGauges[key]),
			attribute.Int64("pbeo.raft.learn_worker.gauge."+key+".max", w.maxGauges[key]),
		)
	}

	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		"pbeo.raft.learn_worker.window",
		trace.WithTimestamp(w.start),
		trace.WithAttributes(attrs...),
	)
	span.End(trace.WithTimestamp(now))
	w.reset(now)
}

func newRaftConsensusBox(cfg BoxConfig, onLearn LearnFunc) (ConsensusBox, error) {
	peerIDs, peers, selfID, err := buildPeerIDs(cfg.Name, cfg.Peers)
	if err != nil {
		return nil, err
	}
	if cfg.SendRaft == nil {
		return nil, fmt.Errorf("pbeo raft consensus box requires a SendRaft callback")
	}
	sendBatch := cfg.SendRaftBatch
	if sendBatch == nil {
		sendBatch = func(peer string, messages []raftpb.Message) error {
			for _, message := range messages {
				if err := cfg.SendRaft(peer, message); err != nil {
					return err
				}
			}
			return nil
		}
	}

	tickInterval := cfg.TickInterval
	if tickInterval <= 0 {
		tickInterval = 10 * time.Millisecond
	}
	electionTick := cfg.ElectionTick
	if electionTick <= 0 {
		electionTick = 10
	}
	heartbeatTick := cfg.HeartbeatTick
	if heartbeatTick <= 0 {
		heartbeatTick = 1
	}
	if heartbeatTick >= electionTick {
		return nil, fmt.Errorf("heartbeat tick must be smaller than election tick")
	}
	maxInflight := cfg.MaxInflightMsgs
	if maxInflight <= 0 {
		maxInflight = 256
	}
	maxSizePerMsg := cfg.MaxSizePerMsg
	if maxSizePerMsg == 0 {
		maxSizePerMsg = 1 << 20
	}
	sendBatchSize := cfg.RaftSendBatchSize
	if sendBatchSize <= 0 {
		sendBatchSize = defaultRaftSendBatchSize
	}
	learnBatchSize := cfg.LearnBatchSize
	if learnBatchSize <= 0 {
		learnBatchSize = defaultRaftLearnBatchSize
	}
	learnBatch := cfg.LearnBatch
	if learnBatch == nil && onLearn != nil {
		learnBatch = func(entries []CommittedEntry) {
			for _, entry := range entries {
				onLearn(entry.Slot, entry.Entry)
			}
		}
	}

	storage := raft.NewMemoryStorage()
	rawNode, err := raft.NewRawNode(&raft.Config{
		ID:              selfID,
		ElectionTick:    electionTick,
		HeartbeatTick:   heartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflight,
	})
	if err != nil {
		return nil, err
	}

	bootstrapPeers := make([]raft.Peer, 0, len(peerIDs))
	peerNames := make([]string, 0, len(peerIDs))
	for peer := range peerIDs {
		peerNames = append(peerNames, peer)
	}
	sort.Strings(peerNames)
	for _, peer := range peerNames {
		bootstrapPeers = append(bootstrapPeers, raft.Peer{ID: peerIDs[peer]})
	}
	if err := rawNode.Bootstrap(bootstrapPeers); err != nil {
		return nil, err
	}

	box := &raftConsensusBox{
		name:            cfg.Name,
		selfID:          selfID,
		peerIDs:         peerIDs,
		peers:           peers,
		send:            cfg.SendRaft,
		sendBatch:       sendBatch,
		learn:           onLearn,
		learnBatch:      learnBatch,
		proposals:       make(chan proposalRequest, raftProposalQueueSize),
		prioritySteps:   make(chan stepRequest, 1024),
		backgroundSteps: make(chan stepRequest, 1024),
		sendQueues:      make(map[uint64]chan raftpb.Message),
		sendBatchSize:   sendBatchSize,
		learnBatchSize:  learnBatchSize,
		learnQueue:      make(chan asyncLearnRequest, raftAsyncLearnQueueSize),
		unreachable:     make(chan uint64, 1024),
		spans:           make(map[string]trace.Span),
		commitWaitSpans: make(map[string]trace.Span),
		appendAckSpans:  make(map[uint64][]pendingAppendSignal),
		ackCommitSpans:  make(map[uint64][]pendingCommitSignal),
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
	}

	for id := range peers {
		if id == selfID {
			continue
		}
		queue := make(chan raftpb.Message, raftAsyncSendQueueSize)
		box.sendQueues[id] = queue
		go box.runSendWorker(id, queue)
	}
	go box.runLearnWorker()
	go box.run(rawNode, storage, tickInterval, cfg.DisableFollowerElections)
	return box, nil
}

func (b *raftConsensusBox) IsLeader() bool {
	return b.leaderID.Load() == b.selfID && b.selfID != 0
}

func (b *raftConsensusBox) Leader() (string, bool) {
	leaderID := b.leaderID.Load()
	if leaderID == 0 {
		return "", false
	}
	peer, ok := b.peers[leaderID]
	return peer, ok
}

func (b *raftConsensusBox) Propose(entry Entry) error {
	result := make(chan error, 1)
	span := b.startRequestTrace(entry)
	request := proposalRequest{
		entry:  entry,
		result: result,
		span:   span,
	}
	channelSpan := b.startProposeChannelSendTrace(entry)
	if err := b.enqueueProposal(request); err != nil {
		endRequestTrace(channelSpan, "stopped_before_enqueue")
		endRequestTrace(span, "stopped_before_enqueue")
		return err
	}
	endRequestTrace(channelSpan, "proposal_enqueued")
	addRequestTraceEvent(span, "proposal_enqueued")

	waitSpan := b.startProposeResultWaitTrace(entry)
	select {
	case <-b.doneCh:
		endRequestTrace(waitSpan, "stopped_before_result")
		return errConsensusBoxStopped
	case err := <-result:
		if err != nil && waitSpan != nil {
			waitSpan.RecordError(err)
		}
		endRequestTrace(waitSpan, "result_received")
		return err
	}
}

func (b *raftConsensusBox) ProposeAsync(entry Entry) error {
	span := b.startRequestTrace(entry)
	request := proposalRequest{
		entry: entry,
		span:  span,
	}
	channelSpan := b.startProposeChannelSendTrace(entry)
	if err := b.enqueueProposal(request); err != nil {
		endRequestTrace(channelSpan, "stopped_before_enqueue")
		endRequestTrace(span, "stopped_before_enqueue")
		return err
	}
	endRequestTrace(channelSpan, "proposal_enqueued")
	addRequestTraceEvent(span, "proposal_enqueued")
	return nil
}

func (b *raftConsensusBox) enqueueProposal(request proposalRequest) error {
	select {
	case <-b.stopCh:
		return errConsensusBoxStopped
	case <-b.doneCh:
		return errConsensusBoxStopped
	default:
	}

	select {
	case <-b.stopCh:
		return errConsensusBoxStopped
	case <-b.doneCh:
		return errConsensusBoxStopped
	case b.proposals <- request:
		return nil
	}
}

func (b *raftConsensusBox) HandleMessage(message raftpb.Message) error {
	request := stepRequest{message: message}
	queue := b.stepQueue(message)
	select {
	case <-b.doneCh:
		return errConsensusBoxStopped
	default:
	}

	if message.Type == raftpb.MsgHeartbeatResp {
		select {
		case <-b.doneCh:
			return errConsensusBoxStopped
		case queue <- request:
			return nil
		default:
			return nil
		}
	}

	select {
	case <-b.doneCh:
		return errConsensusBoxStopped
	case queue <- request:
		return nil
	}
}

func (b *raftConsensusBox) Stop() {
	b.stopOnce.Do(func() {
		close(b.stopCh)
		<-b.doneCh
	})
}

func (b *raftConsensusBox) runSendWorker(peerID uint64, queue <-chan raftpb.Message) {
	peer := b.peers[peerID]
	for {
		select {
		case <-b.stopCh:
			return
		case message := <-queue:
			if b.sendBatchSize <= 1 {
				if err := b.send(peer, message); err != nil {
					select {
					case b.unreachable <- peerID:
					case <-b.stopCh:
					}
				}
				continue
			}
			messages := []raftpb.Message{message}
		drain:
			for len(messages) < b.sendBatchSize {
				select {
				case next := <-queue:
					messages = append(messages, next)
				default:
					break drain
				}
			}
			if err := b.sendBatch(peer, messages); err != nil {
				select {
				case b.unreachable <- peerID:
				case <-b.stopCh:
				}
			}
		}
	}
}

func (b *raftConsensusBox) runLearnWorker() {
	batch := make([]asyncLearnRequest, 0, b.learnBatchSize)
	committed := make([]CommittedEntry, 0, b.learnBatchSize)
	workerTrace := newRaftLearnWorkerTraceWindowStats(b.name, b.selfID)
	defer workerTrace.flush(b.leaderID.Load(), "stop")

	for {
		select {
		case <-b.stopCh:
			return
		case request := <-b.learnQueue:
			batch = append(batch[:0], request)
			queueDepthBeforeDrain := len(b.learnQueue)
		drain:
			for len(batch) < b.learnBatchSize {
				select {
				case <-b.stopCh:
					return
				case next := <-b.learnQueue:
					batch = append(batch, next)
				default:
					break drain
				}
			}
			queueDepthAfterDrain := len(b.learnQueue)

			callbackStart := time.Now()
			if b.learnBatch != nil {
				committed = committed[:0]
				for _, request := range batch {
					committed = append(committed, CommittedEntry{Slot: request.slot, Entry: request.entry})
				}
				b.learnBatch(committed)
			} else if b.learn != nil {
				for _, request := range batch {
					b.learn(request.slot, request.entry)
				}
			}
			workerTrace.recordBatch(batch, queueDepthBeforeDrain, queueDepthAfterDrain, time.Since(callbackStart))
			workerTrace.maybeFlush(b.leaderID.Load(), "window")
		}
	}
}

func (b *raftConsensusBox) run(rawNode *raft.RawNode, storage *raft.MemoryStorage, tickInterval time.Duration, disableFollowerElections bool) {
	defer b.endOpenRequestTraces("box_stopped")
	defer b.endQueuedProposalTraces("box_stopped")
	defer close(b.doneCh)

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	loopTrace := newRaftLoopTraceWindowStats("pbeo", b.name, b.selfID)
	defer loopTrace.flush(b.leaderID.Load(), "stop")

	b.drainReady(rawNode, storage)

	for {
		selectStart := time.Now()
		select {
		case request := <-b.prioritySteps:
			selectWait := time.Since(selectStart)
			handlerStart := time.Now()
			stepDuration, drainStats, drainDuration := b.processStep(rawNode, storage, request)
			handlerDuration := time.Since(handlerStart)
			loopTrace.addCount("iterations", 1)
			loopTrace.addCount("priority_steps", 1)
			loopTrace.addDuration("select_wait", selectWait)
			loopTrace.addDuration("handler", handlerDuration)
			loopTrace.addDuration("raw_step", stepDuration)
			loopTrace.recordDrain(drainStats, drainDuration)
			loopTrace.recordQueues(len(b.proposals), len(b.prioritySteps), len(b.backgroundSteps), len(b.unreachable), len(b.learnQueue))
			loopTrace.maybeFlush(b.leaderID.Load(), "window")
			continue
		default:
		}

		selectStart = time.Now()
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			selectWait := time.Since(selectStart)
			handlerStart := time.Now()
			if shouldTickRaft(rawNode.Status(), disableFollowerElections) {
				tickStart := time.Now()
				rawNode.Tick()
				tickDuration := time.Since(tickStart)
				drainStats, drainDuration := b.drainReady(rawNode, storage)
				loopTrace.addDuration("raw_tick", tickDuration)
				loopTrace.recordDrain(drainStats, drainDuration)
				loopTrace.addCount("ticks", 1)
			} else {
				loopTrace.addCount("ticks_skipped", 1)
			}
			loopTrace.addCount("iterations", 1)
			loopTrace.addDuration("select_wait", selectWait)
			loopTrace.addDuration("handler", time.Since(handlerStart))
			loopTrace.recordQueues(len(b.proposals), len(b.prioritySteps), len(b.backgroundSteps), len(b.unreachable), len(b.learnQueue))
			loopTrace.maybeFlush(b.leaderID.Load(), "window")
		case request := <-b.proposals:
			selectWait := time.Since(selectStart)
			handlerStart := time.Now()
			proposalSpan := b.startProposalLoopTrace(request.entry)
			b.rememberRequestTrace(request.entry, request.span)
			marshalStart := time.Now()
			data, err := json.Marshal(request.entry)
			marshalDuration := time.Since(marshalStart)
			var proposeDuration time.Duration
			var drainStats drainReadyStats
			var drainDuration time.Duration
			if err == nil {
				addRequestTraceEvent(request.span, "propose")
				proposeStart := time.Now()
				err = rawNode.Propose(data)
				proposeDuration = time.Since(proposeStart)
				if err != nil {
					endRequestTrace(request.span, "propose_error")
					b.forgetRequestTrace(request.entry)
				}
				drainStats, drainDuration = b.drainReady(rawNode, storage)
				if err == nil {
					b.startCommitWaitTrace(request.entry)
				}
			} else {
				endRequestTrace(request.span, "marshal_error")
				b.forgetRequestTrace(request.entry)
			}
			if request.result != nil {
				request.result <- err
			}
			if err != nil && proposalSpan != nil {
				proposalSpan.RecordError(err)
			}
			endRequestTrace(proposalSpan, "result_sent")
			loopTrace.addCount("iterations", 1)
			loopTrace.addCount("proposals", 1)
			if err != nil {
				loopTrace.addCount("proposal_errors", 1)
			}
			loopTrace.addDuration("select_wait", selectWait)
			loopTrace.addDuration("handler", time.Since(handlerStart))
			loopTrace.addDuration("marshal", marshalDuration)
			loopTrace.addDuration("raw_propose", proposeDuration)
			loopTrace.recordDrain(drainStats, drainDuration)
			loopTrace.recordQueues(len(b.proposals), len(b.prioritySteps), len(b.backgroundSteps), len(b.unreachable), len(b.learnQueue))
			loopTrace.maybeFlush(b.leaderID.Load(), "window")
		case peerID := <-b.unreachable:
			selectWait := time.Since(selectStart)
			handlerStart := time.Now()
			rawNode.ReportUnreachable(peerID)
			drainStats, drainDuration := b.drainReady(rawNode, storage)
			loopTrace.addCount("iterations", 1)
			loopTrace.addCount("unreachable_reports", 1)
			loopTrace.addDuration("select_wait", selectWait)
			loopTrace.addDuration("handler", time.Since(handlerStart))
			loopTrace.recordDrain(drainStats, drainDuration)
			loopTrace.recordQueues(len(b.proposals), len(b.prioritySteps), len(b.backgroundSteps), len(b.unreachable), len(b.learnQueue))
			loopTrace.maybeFlush(b.leaderID.Load(), "window")
		case request := <-b.prioritySteps:
			selectWait := time.Since(selectStart)
			handlerStart := time.Now()
			stepDuration, drainStats, drainDuration := b.processStep(rawNode, storage, request)
			loopTrace.addCount("iterations", 1)
			loopTrace.addCount("priority_steps", 1)
			loopTrace.addDuration("select_wait", selectWait)
			loopTrace.addDuration("handler", time.Since(handlerStart))
			loopTrace.addDuration("raw_step", stepDuration)
			loopTrace.recordDrain(drainStats, drainDuration)
			loopTrace.recordQueues(len(b.proposals), len(b.prioritySteps), len(b.backgroundSteps), len(b.unreachable), len(b.learnQueue))
			loopTrace.maybeFlush(b.leaderID.Load(), "window")
		case request := <-b.backgroundSteps:
			selectWait := time.Since(selectStart)
			handlerStart := time.Now()
			stepDuration, drainStats, drainDuration := b.processStep(rawNode, storage, request)
			loopTrace.addCount("iterations", 1)
			loopTrace.addCount("background_steps", 1)
			loopTrace.addDuration("select_wait", selectWait)
			loopTrace.addDuration("handler", time.Since(handlerStart))
			loopTrace.addDuration("raw_step", stepDuration)
			loopTrace.recordDrain(drainStats, drainDuration)
			loopTrace.recordQueues(len(b.proposals), len(b.prioritySteps), len(b.backgroundSteps), len(b.unreachable), len(b.learnQueue))
			loopTrace.maybeFlush(b.leaderID.Load(), "window")
		}
	}
}

func shouldTickRaft(status raft.Status, disableFollowerElections bool) bool {
	if !disableFollowerElections {
		return true
	}
	if status.RaftState == raft.StateLeader {
		return true
	}
	return status.Lead == 0
}

func (b *raftConsensusBox) endQueuedProposalTraces(event string) {
	for {
		select {
		case request := <-b.proposals:
			if request.result != nil {
				select {
				case request.result <- errConsensusBoxStopped:
				default:
				}
			}
			endRequestTrace(request.span, event)
		default:
			return
		}
	}
}

func (b *raftConsensusBox) processStep(rawNode *raft.RawNode, storage *raft.MemoryStorage, request stepRequest) (time.Duration, drainReadyStats, time.Duration) {
	message := request.message
	commitBefore := rawNode.Status().Commit
	stepStart := time.Now()
	if err := rawNode.Step(message); err == nil {
		stepDuration := time.Since(stepStart)
		b.finishAppendAckSignals(message)
		b.trackAckToCommitSignal(message, commitBefore, rawNode.Status().Commit)
		drainStats, drainDuration := b.drainReady(rawNode, storage)
		return stepDuration, drainStats, drainDuration
	}
	return time.Since(stepStart), drainReadyStats{}, 0
}

func (b *raftConsensusBox) drainReady(rawNode *raft.RawNode, storage *raft.MemoryStorage) (drainReadyStats, time.Duration) {
	start := time.Now()
	stats := drainReadyStats{}

	for rawNode.HasReady() {
		ready := rawNode.Ready()
		stats.readyBatches++
		stats.entries += len(ready.Entries)
		stats.committedEntries += len(ready.CommittedEntries)
		stats.messages += len(ready.Messages)

		if ready.SoftState != nil {
			b.leaderID.Store(ready.SoftState.Lead)
			stats.leaderTransitions++
		}
		if !raft.IsEmptySnap(ready.Snapshot) {
			stats.snapshots++
			storageStart := time.Now()
			_ = storage.ApplySnapshot(ready.Snapshot)
			stats.storageNanos += time.Since(storageStart).Nanoseconds()
		}
		if !raft.IsEmptyHardState(ready.HardState) {
			stats.hardStates++
			storageStart := time.Now()
			_ = storage.SetHardState(ready.HardState)
			stats.storageNanos += time.Since(storageStart).Nanoseconds()
		}
		if len(ready.Entries) > 0 {
			storageStart := time.Now()
			_ = storage.Append(ready.Entries)
			stats.storageNanos += time.Since(storageStart).Nanoseconds()
		}

		for _, message := range ready.Messages {
			if message.To == b.selfID {
				stats.selfMessages++
				selfStepStart := time.Now()
				_ = rawNode.Step(message)
				stats.selfStepNanos += time.Since(selfStepStart).Nanoseconds()
				continue
			}
			_, ok := b.peers[message.To]
			if !ok {
				continue
			}
			queue := b.sendQueues[message.To]
			if queue == nil {
				continue
			}
			sendStart := time.Now()
			select {
			case <-b.stopCh:
				return stats, time.Since(start)
			case queue <- message:
			}
			stats.sendEnqueueNanos += time.Since(sendStart).Nanoseconds()
			b.trackAppendAckSignals(message)
			if isCommitIndexMessage(message) {
				b.finishCommitSignals(message.To, message.Commit)
			}
			stats.sentMessages++
		}

		for _, entry := range ready.CommittedEntries {
			learnStart := time.Now()
			b.learnCommittedEntry(rawNode, entry, &stats)
			stats.learnNanos += time.Since(learnStart).Nanoseconds()
		}

		advanceStart := time.Now()
		rawNode.Advance(ready)
		stats.advanceNanos += time.Since(advanceStart).Nanoseconds()
	}
	return stats, time.Since(start)
}

func (b *raftConsensusBox) stepQueue(message raftpb.Message) chan stepRequest {
	if isPriorityStep(message.Type) {
		return b.prioritySteps
	}
	return b.backgroundSteps
}

func isPriorityStep(messageType raftpb.MessageType) bool {
	switch messageType {
	case raftpb.MsgApp, raftpb.MsgAppResp, raftpb.MsgVote, raftpb.MsgVoteResp, raftpb.MsgPreVote, raftpb.MsgPreVoteResp:
		return true
	default:
		return false
	}
}

func (b *raftConsensusBox) learnCommittedEntry(rawNode *raft.RawNode, entry raftpb.Entry, stats *drainReadyStats) {
	switch entry.Type {
	case raftpb.EntryConfChange:
		if stats != nil {
			stats.confChanges++
		}
		if len(entry.Data) == 0 {
			return
		}
		var change raftpb.ConfChange
		if err := change.Unmarshal(entry.Data); err == nil {
			rawNode.ApplyConfChange(change)
		}
	case raftpb.EntryConfChangeV2:
		if stats != nil {
			stats.confChanges++
		}
		if len(entry.Data) == 0 {
			return
		}
		var change raftpb.ConfChangeV2
		if err := change.Unmarshal(entry.Data); err == nil {
			rawNode.ApplyConfChange(change)
		}
	case raftpb.EntryNormal:
		if stats != nil {
			stats.normalEntries++
		}
		if len(entry.Data) == 0 {
			return
		}
		var value Entry
		decodeStart := time.Now()
		err := json.Unmarshal(entry.Data, &value)
		if stats != nil {
			stats.learnDecodeNanos += time.Since(decodeStart).Nanoseconds()
		}
		if err != nil {
			return
		}
		b.learnedSlot++
		b.finishCommittedRequestTrace(b.learnedSlot, value)
		cloneStart := time.Now()
		cloned := cloneEntry(value)
		if stats != nil {
			stats.learnCloneNanos += time.Since(cloneStart).Nanoseconds()
		}
		request := asyncLearnRequest{slot: b.learnedSlot, entry: cloned, enqueued: time.Now()}
		enqueueStart := time.Now()
		select {
		case <-b.stopCh:
			return
		case b.learnQueue <- request:
		}
		if stats != nil {
			stats.learnEnqueueNanos += time.Since(enqueueStart).Nanoseconds()
		}
	}
}

func (b *raftConsensusBox) startRequestTrace(entry Entry) trace.Span {
	if entry.RequestID == "" || !telemetry.DetailedSpansEnabled() {
		return nil
	}
	_, span := telemetry.StartSpanFromPayload(
		entry.Response,
		"pbeo.raft.request_to_commit",
		append(
			b.entryTraceAttrs(entry),
			attribute.String("pbeo.raft.trace_scope", "local_propose"),
		)...,
	)
	addRequestTraceEvent(span, "request")
	return span
}

func (b *raftConsensusBox) startProposeChannelSendTrace(entry Entry) trace.Span {
	if entry.RequestID == "" || !telemetry.DetailedSpansEnabled() {
		return nil
	}
	_, span := telemetry.StartSpanFromPayload(
		entry.Response,
		"pbeo.raft.propose_channel_send",
		append(
			b.entryTraceAttrs(entry),
			attribute.Int("pbeo.raft.proposals_queued_before_send", len(b.proposals)),
		)...,
	)
	return span
}

func (b *raftConsensusBox) startProposeResultWaitTrace(entry Entry) trace.Span {
	if entry.RequestID == "" || !telemetry.DetailedSpansEnabled() {
		return nil
	}
	_, span := telemetry.StartSpanFromPayload(
		entry.Response,
		"pbeo.raft.propose_result_wait",
		append(
			b.entryTraceAttrs(entry),
			attribute.Int("pbeo.raft.proposals_queued_after_send", len(b.proposals)),
		)...,
	)
	return span
}

func (b *raftConsensusBox) rememberRequestTrace(entry Entry, span trace.Span) {
	if span == nil || entry.RequestID == "" {
		return
	}
	b.spans[entry.RequestID] = span
	addRequestTraceEvent(span, "proposal_received")
}

func (b *raftConsensusBox) forgetRequestTrace(entry Entry) {
	if entry.RequestID == "" {
		return
	}
	delete(b.spans, entry.RequestID)
}

func (b *raftConsensusBox) finishCommittedRequestTrace(slot uint64, entry Entry) {
	if entry.RequestID == "" || !telemetry.DetailedSpansEnabled() {
		return
	}
	b.finishCommitWaitTrace(slot, entry)
	span, ok := b.spans[entry.RequestID]
	if !ok {
		_, span = telemetry.StartSpanFromPayload(
			entry.Response,
			"pbeo.raft.request_to_commit",
			append(
				b.entryTraceAttrs(entry),
				attribute.Int64("pbeo.raft.commit_slot", int64(slot)),
				attribute.String("pbeo.raft.trace_scope", "commit_observed"),
			)...,
		)
		endRequestTrace(span, "commit_observed")
		return
	}
	delete(b.spans, entry.RequestID)
	span.SetAttributes(
		attribute.Int64("pbeo.raft.commit_slot", int64(slot)),
		attribute.String("pbeo.raft.trace_scope", "local_propose"),
	)
	endRequestTrace(span, "commit")
}

func (b *raftConsensusBox) endOpenRequestTraces(event string) {
	for requestID, span := range b.spans {
		endRequestTrace(span, event)
		delete(b.spans, requestID)
	}
	for requestID, span := range b.commitWaitSpans {
		endRequestTrace(span, event)
		delete(b.commitWaitSpans, requestID)
	}
	for peerID, signals := range b.appendAckSpans {
		for _, signal := range signals {
			endRequestTrace(signal.span, event)
		}
		delete(b.appendAckSpans, peerID)
	}
	for peerID, signals := range b.ackCommitSpans {
		for _, signal := range signals {
			endRequestTrace(signal.span, event)
		}
		delete(b.ackCommitSpans, peerID)
	}
}

func (b *raftConsensusBox) startProposalLoopTrace(entry Entry) trace.Span {
	if entry.RequestID == "" || !telemetry.DetailedSpansEnabled() {
		return nil
	}
	_, span := telemetry.StartSpanFromPayload(
		entry.Response,
		"pbeo.raft.proposal_loop",
		append(
			b.entryTraceAttrs(entry),
			attribute.Int("pbeo.raft.proposals_queued_after_dequeue", len(b.proposals)),
			attribute.Int("pbeo.raft.priority_steps_queued", len(b.prioritySteps)),
			attribute.Int("pbeo.raft.background_steps_queued", len(b.backgroundSteps)),
		)...,
	)
	return span
}

func (b *raftConsensusBox) startCommitWaitTrace(entry Entry) {
	if entry.RequestID == "" || !telemetry.DetailedSpansEnabled() {
		return
	}
	_, span := telemetry.StartSpanFromPayload(
		entry.Response,
		"pbeo.raft.wait_commit_after_propose",
		append(
			b.entryTraceAttrs(entry),
			attribute.Int("pbeo.raft.priority_steps_queued_after_propose", len(b.prioritySteps)),
			attribute.Int("pbeo.raft.background_steps_queued_after_propose", len(b.backgroundSteps)),
		)...,
	)
	b.commitWaitSpans[entry.RequestID] = span
}

func (b *raftConsensusBox) finishCommitWaitTrace(slot uint64, entry Entry) {
	span := b.commitWaitSpans[entry.RequestID]
	if span == nil {
		return
	}
	delete(b.commitWaitSpans, entry.RequestID)
	span.SetAttributes(attribute.Int64("pbeo.raft.commit_slot", int64(slot)))
	endRequestTrace(span, "commit")
}

func (b *raftConsensusBox) trackAppendAckSignals(message raftpb.Message) {
	if !telemetry.DetailedSpansEnabled() || message.Type != raftpb.MsgApp || len(message.Entries) == 0 || !b.IsLeader() {
		return
	}
	peer := b.peers[message.To]
	for _, entry := range message.Entries {
		if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
			continue
		}
		var value Entry
		if err := json.Unmarshal(entry.Data, &value); err != nil || value.RequestID == "" {
			continue
		}
		_, span := telemetry.StartSpanFromPayload(
			value.Response,
			"pbeo.raft.append_send_to_ack",
			append(
				b.entryTraceAttrs(value),
				attribute.String("raft.peer", peer),
				attribute.Int64("raft.message.to", int64(message.To)),
				attribute.Int64("raft.message.index", int64(message.Index)),
				attribute.Int64("raft.entry.index", int64(entry.Index)),
			)...,
		)
		b.appendAckSpans[message.To] = append(b.appendAckSpans[message.To], pendingAppendSignal{
			index:     entry.Index,
			requestID: value.RequestID,
			span:      span,
		})
	}
}

func (b *raftConsensusBox) finishAppendAckSignals(message raftpb.Message) {
	if message.Type != raftpb.MsgAppResp || message.Reject || message.Index == 0 || !b.IsLeader() {
		return
	}
	pending := b.appendAckSpans[message.From]
	if len(pending) == 0 {
		return
	}
	remaining := pending[:0]
	var finished []pendingAppendSignal
	for _, signal := range pending {
		if signal.index <= message.Index {
			finished = append(finished, signal)
			continue
		}
		remaining = append(remaining, signal)
	}
	if len(remaining) == 0 {
		delete(b.appendAckSpans, message.From)
	} else {
		b.appendAckSpans[message.From] = remaining
	}
	for _, signal := range finished {
		signal.span.SetAttributes(attribute.Int64("raft.message.ack_index", int64(message.Index)))
		endRequestTrace(signal.span, "ack_processed")
	}
}

func (b *raftConsensusBox) trackAckToCommitSignal(message raftpb.Message, commitBefore uint64, commitAfter uint64) {
	if !telemetry.DetailedSpansEnabled() || message.Type != raftpb.MsgAppResp || message.Reject || message.Index == 0 || !b.IsLeader() {
		return
	}
	peer := b.peers[message.From]
	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		"pbeo.raft.ack_to_commit_send",
		trace.WithAttributes(
			attribute.String("node.name", b.name),
			attribute.Int64("raft.node_id", int64(b.selfID)),
			attribute.Int64("raft.leader_id", int64(b.leaderID.Load())),
			attribute.String("raft.peer", peer),
			attribute.Int64("raft.message.from", int64(message.From)),
			attribute.Int64("raft.message.to", int64(message.To)),
			attribute.Int64("raft.message.index", int64(message.Index)),
			attribute.Int64("raft.status.commit_before_step", int64(commitBefore)),
			attribute.Int64("raft.status.commit_after_step", int64(commitAfter)),
		),
	)
	b.ackCommitSpans[message.From] = append(b.ackCommitSpans[message.From], pendingCommitSignal{
		index: message.Index,
		span:  span,
	})
}

func (b *raftConsensusBox) finishCommitSignals(peerID uint64, commit uint64) {
	if commit == 0 {
		return
	}
	pending := b.ackCommitSpans[peerID]
	if len(pending) == 0 {
		return
	}
	remaining := pending[:0]
	var finished []pendingCommitSignal
	for _, signal := range pending {
		if signal.index <= commit {
			finished = append(finished, signal)
			continue
		}
		remaining = append(remaining, signal)
	}
	if len(remaining) == 0 {
		delete(b.ackCommitSpans, peerID)
	} else {
		b.ackCommitSpans[peerID] = remaining
	}
	for _, signal := range finished {
		signal.span.SetAttributes(attribute.Int64("raft.message.commit", int64(commit)))
		endRequestTrace(signal.span, "commit_index_enqueued")
	}
}

func isCommitIndexMessage(message raftpb.Message) bool {
	if message.Commit == 0 {
		return false
	}
	switch message.Type {
	case raftpb.MsgApp, raftpb.MsgHeartbeat:
		return true
	default:
		return false
	}
}

func (b *raftConsensusBox) entryTraceAttrs(entry Entry) []attribute.KeyValue {
	attrs := append([]attribute.KeyValue{}, telemetry.AttrsFromPayload(entry.Response)...)
	attrs = append(attrs,
		attribute.String("node.name", b.name),
		attribute.Int64("raft.node_id", int64(b.selfID)),
	)
	if entry.RequestID != "" {
		attrs = append(attrs, attribute.String("request.id", entry.RequestID))
	}
	return attrs
}

func addRequestTraceEvent(span trace.Span, event string) {
	if span != nil {
		span.AddEvent(event)
	}
}

func endRequestTrace(span trace.Span, event string) {
	if span == nil {
		return
	}
	span.AddEvent(event)
	span.End()
}

func buildPeerIDs(self string, peers []string) (map[string]uint64, map[uint64]string, uint64, error) {
	if self == "" {
		return nil, nil, 0, fmt.Errorf("pbeo raft consensus box requires a node name")
	}
	if len(peers) == 0 {
		return nil, nil, 0, fmt.Errorf("pbeo raft consensus box requires peers")
	}

	uniquePeers := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		if peer == "" {
			continue
		}
		uniquePeers[peer] = struct{}{}
	}
	if _, ok := uniquePeers[self]; !ok {
		uniquePeers[self] = struct{}{}
	}

	peerNames := make([]string, 0, len(uniquePeers))
	for peer := range uniquePeers {
		peerNames = append(peerNames, peer)
	}
	sort.Strings(peerNames)

	peerIDs := make(map[string]uint64, len(peerNames))
	reverse := make(map[uint64]string, len(peerNames))
	for index, peer := range peerNames {
		id := uint64(index + 1)
		peerIDs[peer] = id
		reverse[id] = peer
	}
	return peerIDs, reverse, peerIDs[self], nil
}
