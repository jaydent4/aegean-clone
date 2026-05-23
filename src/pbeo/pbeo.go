package pbeo

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"aegean/common"
	"aegean/eo"
	"aegean/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type candidateSubmission struct {
	requestID string
	response  map[string]any
	writes    map[string]string
}

type asyncConsensusBox interface {
	ProposeAsync(entry Entry) error
}

type committedResponseTask struct {
	entry    Entry
	enqueued time.Time
}

type committedResponseQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	stopped bool
	items   []committedResponseTask
	head    int
}

func newCommittedResponseQueue() *committedResponseQueue {
	q := &committedResponseQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *committedResponseQueue) enqueue(task committedResponseTask) (int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return q.lenLocked(), false
	}
	q.items = append(q.items, task)
	depth := q.lenLocked()
	q.cond.Signal()
	return depth, true
}

func (q *committedResponseQueue) dequeue() (committedResponseTask, int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.lenLocked() == 0 && !q.stopped {
		q.cond.Wait()
	}
	if q.stopped || q.lenLocked() == 0 {
		return committedResponseTask{}, 0, false
	}
	task := q.items[q.head]
	q.items[q.head] = committedResponseTask{}
	q.head++
	depth := q.lenLocked()
	if q.head > 1024 && q.head*2 >= len(q.items) {
		q.items = append([]committedResponseTask(nil), q.items[q.head:]...)
		q.head = 0
	}
	return task, depth, true
}

func (q *committedResponseQueue) stop() {
	q.mu.Lock()
	q.stopped = true
	q.cond.Broadcast()
	q.mu.Unlock()
}

func (q *committedResponseQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.lenLocked()
}

func (q *committedResponseQueue) lenLocked() int {
	return len(q.items) - q.head
}

type pbeoApplyStats struct {
	entries              int64
	writes               int64
	writeBytes           int64
	responsesEnqueued    int64
	stateWriteNanos      int64
	stateWriteWaitNanos  int64
	stateWriteHoldNanos  int64
	clearStateNanos      int64
	responseEnqueueNanos int64
}

type pbeoProcessStats struct {
	entries          int64
	processNanos     int64
	processLockNanos int64
	dequeueNanos     int64
	applyNanos       int64
	apply            pbeoApplyStats
}

type pbeoLearnBatchWindowStats struct {
	nodeName       string
	start          time.Time
	counts         map[string]int64
	durations      map[string]time.Duration
	durationCounts map[string]int64
	maxDurations   map[string]time.Duration
}

type pbeoStateLockWindowStats struct {
	nodeName       string
	start          time.Time
	counts         map[string]int64
	durations      map[string]time.Duration
	durationCounts map[string]int64
	maxDurations   map[string]time.Duration
	lastGauges     map[string]int64
	maxGauges      map[string]int64
}

type pbeoResponseWindowStats struct {
	nodeName       string
	start          time.Time
	counts         map[string]int64
	durations      map[string]time.Duration
	durationCounts map[string]int64
	maxDurations   map[string]time.Duration
	lastGauges     map[string]int64
	maxGauges      map[string]int64
}

type PBEO struct {
	name      string
	peers     []string
	clients   []string
	execute   ExecuteRequestFunc
	send      SendFunc
	box       ConsensusBox
	nestedEO  *eo.EO
	runConfig map[string]any

	processMu         sync.Mutex
	mu                sync.Mutex
	log               map[uint64]Entry
	learned           uint64
	processed         uint64
	learnedSlots      map[uint64]struct{}
	requestAttempts   map[string]struct{}
	committedRequests map[string]Entry
	blockedRequests   map[string]map[string]any
	nestedProposals   map[string]struct{}

	stateMu           sync.RWMutex
	kv                map[string]string
	executingRequests atomic.Int64

	commitCh        chan candidateSubmission
	responseQueue   *committedResponseQueue
	responseStop    chan struct{}
	responseStopper sync.Once
	nestedResponses *nestedResponseStore
	contextStore    *requestContextStore
	lifecycleSpans  map[string]trace.Span
	executeSlots    chan struct{}
	commitDrainSize int
	learnTraceMu    sync.Mutex
	learnTrace      *pbeoLearnBatchWindowStats
	stateTraceMu    sync.Mutex
	stateTrace      *pbeoStateLockWindowStats
	responseTraceMu sync.Mutex
	responseTrace   *pbeoResponseWindowStats
}

const (
	commitQueueSize             = 4096
	committedResponseWorkerSize = 16
)

func newPBEOLearnBatchWindowStats(nodeName string) *pbeoLearnBatchWindowStats {
	w := &pbeoLearnBatchWindowStats{nodeName: nodeName}
	w.reset(time.Now())
	return w
}

func (w *pbeoLearnBatchWindowStats) reset(start time.Time) {
	w.start = start
	w.counts = make(map[string]int64)
	w.durations = make(map[string]time.Duration)
	w.durationCounts = make(map[string]int64)
	w.maxDurations = make(map[string]time.Duration)
}

func (w *pbeoLearnBatchWindowStats) addCount(name string, value int64) {
	if value == 0 {
		return
	}
	w.counts[name] += value
}

func (w *pbeoLearnBatchWindowStats) addDuration(name string, duration time.Duration) {
	if duration <= 0 {
		return
	}
	w.durations[name] += duration
	w.durationCounts[name]++
	if duration > w.maxDurations[name] {
		w.maxDurations[name] = duration
	}
}

func (w *pbeoLearnBatchWindowStats) record(entries int, learned int, duplicates int, logUpdate time.Duration, processStats pbeoProcessStats, total time.Duration) {
	w.addCount("batches", 1)
	w.addCount("entries", int64(entries))
	w.addCount("learned_entries", int64(learned))
	w.addCount("duplicate_entries", int64(duplicates))
	w.addCount("processed_entries", processStats.entries)
	w.addCount("writes", processStats.apply.writes)
	w.addCount("write_bytes", processStats.apply.writeBytes)
	w.addCount("responses_enqueued", processStats.apply.responsesEnqueued)
	w.addDuration("total", total)
	w.addDuration("log_update", logUpdate)
	w.addDuration("process", time.Duration(processStats.processNanos))
	w.addDuration("process_lock", time.Duration(processStats.processLockNanos))
	w.addDuration("dequeue", time.Duration(processStats.dequeueNanos))
	w.addDuration("apply", time.Duration(processStats.applyNanos))
	w.addDuration("state_write", time.Duration(processStats.apply.stateWriteNanos))
	w.addDuration("state_write_wait", time.Duration(processStats.apply.stateWriteWaitNanos))
	w.addDuration("state_write_hold", time.Duration(processStats.apply.stateWriteHoldNanos))
	w.addDuration("clear_state", time.Duration(processStats.apply.clearStateNanos))
	w.addDuration("response_enqueue", time.Duration(processStats.apply.responseEnqueueNanos))
}

func (w *pbeoLearnBatchWindowStats) flush(reason string) {
	if w.counts["batches"] == 0 {
		w.reset(time.Now())
		return
	}
	now := time.Now()
	elapsed := now.Sub(w.start)
	attrs := []attribute.KeyValue{
		attribute.String("node.name", w.nodeName),
		attribute.String("pbeo.learn_batch.flush_reason", reason),
		attribute.Int64("pbeo.learn_batch.window_us", elapsed.Microseconds()),
	}
	for _, key := range sortedInt64MapKeys(w.counts) {
		attrs = append(attrs, attribute.Int64("pbeo.learn_batch.count."+key, w.counts[key]))
	}
	for _, key := range sortedDurationMapKeys(w.durations) {
		total := w.durations[key]
		count := w.durationCounts[key]
		avg := time.Duration(0)
		if count > 0 {
			avg = total / time.Duration(count)
		}
		attrs = append(attrs,
			attribute.Int64("pbeo.learn_batch.duration."+key+".count", count),
			attribute.Int64("pbeo.learn_batch.duration."+key+".total_us", total.Microseconds()),
			attribute.Int64("pbeo.learn_batch.duration."+key+".avg_us", avg.Microseconds()),
			attribute.Int64("pbeo.learn_batch.duration."+key+".max_us", w.maxDurations[key].Microseconds()),
		)
	}

	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		"pbeo.learn_batch.window",
		trace.WithTimestamp(w.start),
		trace.WithAttributes(attrs...),
	)
	span.End(trace.WithTimestamp(now))
	w.reset(now)
}

func newPBEOStateLockWindowStats(nodeName string) *pbeoStateLockWindowStats {
	w := &pbeoStateLockWindowStats{nodeName: nodeName}
	w.reset(time.Now())
	return w
}

func (w *pbeoStateLockWindowStats) reset(start time.Time) {
	w.start = start
	w.counts = make(map[string]int64)
	w.durations = make(map[string]time.Duration)
	w.durationCounts = make(map[string]int64)
	w.maxDurations = make(map[string]time.Duration)
	w.lastGauges = make(map[string]int64)
	w.maxGauges = make(map[string]int64)
}

func (w *pbeoStateLockWindowStats) addCount(name string, value int64) {
	if value == 0 {
		return
	}
	w.counts[name] += value
}

func (w *pbeoStateLockWindowStats) addDuration(name string, duration time.Duration) {
	if duration <= 0 {
		return
	}
	w.durations[name] += duration
	w.durationCounts[name]++
	if duration > w.maxDurations[name] {
		w.maxDurations[name] = duration
	}
}

func (w *pbeoStateLockWindowStats) observeGauge(name string, value int64) {
	w.lastGauges[name] = value
	if value > w.maxGauges[name] {
		w.maxGauges[name] = value
	}
}

func (w *pbeoStateLockWindowStats) recordStateRead(wait time.Duration, hold time.Duration, total time.Duration, executingRequests int64) {
	w.addCount("state_read_calls", 1)
	w.addDuration("state_read_wait", wait)
	w.addDuration("state_read_hold", hold)
	w.addDuration("state_read_total", total)
	w.observeGauge("executing_requests", executingRequests)
}

func (w *pbeoStateLockWindowStats) recordStateWrite(entries int64, writes int64, writeBytes int64, wait time.Duration, hold time.Duration, total time.Duration) {
	w.addCount("state_write_batches", 1)
	w.addCount("state_write_entries", entries)
	w.addCount("state_write_writes", writes)
	w.addCount("state_write_bytes", writeBytes)
	w.addDuration("state_write_wait", wait)
	w.addDuration("state_write_hold", hold)
	w.addDuration("state_write_total", total)
}

func (w *pbeoStateLockWindowStats) flush(reason string) {
	if w.counts["state_read_calls"] == 0 && w.counts["state_write_batches"] == 0 {
		w.reset(time.Now())
		return
	}
	now := time.Now()
	elapsed := now.Sub(w.start)
	attrs := []attribute.KeyValue{
		attribute.String("node.name", w.nodeName),
		attribute.String("pbeo.state_lock.flush_reason", reason),
		attribute.Int64("pbeo.state_lock.window_us", elapsed.Microseconds()),
	}
	for _, key := range sortedInt64MapKeys(w.counts) {
		attrs = append(attrs, attribute.Int64("pbeo.state_lock.count."+key, w.counts[key]))
	}
	for _, key := range sortedDurationMapKeys(w.durations) {
		total := w.durations[key]
		count := w.durationCounts[key]
		avg := time.Duration(0)
		if count > 0 {
			avg = total / time.Duration(count)
		}
		attrs = append(attrs,
			attribute.Int64("pbeo.state_lock.duration."+key+".count", count),
			attribute.Int64("pbeo.state_lock.duration."+key+".total_us", total.Microseconds()),
			attribute.Int64("pbeo.state_lock.duration."+key+".avg_us", avg.Microseconds()),
			attribute.Int64("pbeo.state_lock.duration."+key+".max_us", w.maxDurations[key].Microseconds()),
		)
	}
	for _, key := range sortedInt64MapKeys(w.lastGauges) {
		attrs = append(attrs,
			attribute.Int64("pbeo.state_lock.gauge."+key+".last", w.lastGauges[key]),
			attribute.Int64("pbeo.state_lock.gauge."+key+".max", w.maxGauges[key]),
		)
	}

	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		"pbeo.state_lock.window",
		trace.WithTimestamp(w.start),
		trace.WithAttributes(attrs...),
	)
	span.End(trace.WithTimestamp(now))
	w.reset(now)
}

func newPBEOResponseWindowStats(nodeName string) *pbeoResponseWindowStats {
	w := &pbeoResponseWindowStats{nodeName: nodeName}
	w.reset(time.Now())
	return w
}

func (w *pbeoResponseWindowStats) reset(start time.Time) {
	w.start = start
	w.counts = make(map[string]int64)
	w.durations = make(map[string]time.Duration)
	w.durationCounts = make(map[string]int64)
	w.maxDurations = make(map[string]time.Duration)
	w.lastGauges = make(map[string]int64)
	w.maxGauges = make(map[string]int64)
}

func (w *pbeoResponseWindowStats) addCount(name string, value int64) {
	if value == 0 {
		return
	}
	w.counts[name] += value
}

func (w *pbeoResponseWindowStats) addDuration(name string, duration time.Duration) {
	if duration <= 0 {
		return
	}
	w.durations[name] += duration
	w.durationCounts[name]++
	if duration > w.maxDurations[name] {
		w.maxDurations[name] = duration
	}
}

func (w *pbeoResponseWindowStats) observeGauge(name string, value int64) {
	w.lastGauges[name] = value
	if value > w.maxGauges[name] {
		w.maxGauges[name] = value
	}
}

func (w *pbeoResponseWindowStats) recordEnqueue(depth int, duration time.Duration) {
	w.addCount("enqueued", 1)
	w.addDuration("enqueue", duration)
	w.observeGauge("queue_depth", int64(depth))
}

func (w *pbeoResponseWindowStats) recordSend(queueDepth int, clients int, queueWait time.Duration, sendDuration time.Duration) {
	w.addCount("sent_entries", 1)
	w.addCount("sent_clients", int64(clients))
	w.addDuration("queue_wait", queueWait)
	w.addDuration("send", sendDuration)
	w.observeGauge("queue_depth", int64(queueDepth))
}

func (w *pbeoResponseWindowStats) flush(reason string) {
	if w.counts["enqueued"] == 0 && w.counts["sent_entries"] == 0 {
		w.reset(time.Now())
		return
	}
	now := time.Now()
	elapsed := now.Sub(w.start)
	attrs := []attribute.KeyValue{
		attribute.String("node.name", w.nodeName),
		attribute.String("pbeo.response.flush_reason", reason),
		attribute.Int64("pbeo.response.window_us", elapsed.Microseconds()),
	}
	for _, key := range sortedInt64MapKeys(w.counts) {
		attrs = append(attrs, attribute.Int64("pbeo.response.count."+key, w.counts[key]))
	}
	for _, key := range sortedDurationMapKeys(w.durations) {
		total := w.durations[key]
		count := w.durationCounts[key]
		avg := time.Duration(0)
		if count > 0 {
			avg = total / time.Duration(count)
		}
		attrs = append(attrs,
			attribute.Int64("pbeo.response.duration."+key+".count", count),
			attribute.Int64("pbeo.response.duration."+key+".total_us", total.Microseconds()),
			attribute.Int64("pbeo.response.duration."+key+".avg_us", avg.Microseconds()),
			attribute.Int64("pbeo.response.duration."+key+".max_us", w.maxDurations[key].Microseconds()),
		)
	}
	for _, key := range sortedInt64MapKeys(w.lastGauges) {
		attrs = append(attrs,
			attribute.Int64("pbeo.response.gauge."+key+".last", w.lastGauges[key]),
			attribute.Int64("pbeo.response.gauge."+key+".max", w.maxGauges[key]),
		)
	}

	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		"pbeo.response.window",
		trace.WithTimestamp(w.start),
		trace.WithAttributes(attrs...),
	)
	span.End(trace.WithTimestamp(now))
	w.reset(now)
}

func NewPBEO(cfg Config) (*PBEO, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("pbeo requires a node name")
	}
	if len(cfg.Peers) == 0 {
		return nil, fmt.Errorf("pbeo requires at least one peer")
	}
	if cfg.Execute == nil {
		return nil, fmt.Errorf("pbeo requires an execute callback")
	}
	if cfg.Send == nil {
		return nil, fmt.Errorf("pbeo requires a message sender")
	}

	factory := cfg.BoxFactory
	if factory == nil {
		if cfg.SendRaft == nil {
			return nil, fmt.Errorf("pbeo requires a raft sender when using the default consensus box")
		}
		factory = newRaftConsensusBox
	}
	nestedSendRaft := cfg.SendNestedRaft
	nestedSendRaftBatch := cfg.SendNestedRaftBatch
	if cfg.NestedBoxFactory == nil && nestedSendRaft == nil {
		return nil, fmt.Errorf("pbeo requires a nested EO raft sender")
	}

	initial := map[string]string{}
	if cfg.InitState != nil {
		if custom := cfg.InitState(cfg.RunConfig); custom != nil {
			initial = copyStringMap(custom)
		}
	}
	var executeSlots chan struct{}
	if maxExecuting := common.IntOrDefault(cfg.RunConfig, "pbeo_max_executing_requests", 0); maxExecuting > 0 {
		executeSlots = make(chan struct{}, maxExecuting)
	}
	commitDrainSize := common.IntOrDefault(cfg.RunConfig, "pbeo_commit_drain_batch_size", 1)
	if commitDrainSize < 1 {
		commitDrainSize = 1
	}
	p := &PBEO{
		name:              cfg.Name,
		peers:             append([]string{}, cfg.Peers...),
		clients:           append([]string{}, cfg.Clients...),
		execute:           cfg.Execute,
		send:              cfg.Send,
		runConfig:         cloneRunConfig(cfg.RunConfig),
		log:               make(map[uint64]Entry),
		learnedSlots:      make(map[uint64]struct{}),
		requestAttempts:   make(map[string]struct{}),
		committedRequests: make(map[string]Entry),
		blockedRequests:   make(map[string]map[string]any),
		nestedProposals:   make(map[string]struct{}),
		kv:                initial,
		commitCh:          make(chan candidateSubmission, commitQueueSize),
		responseQueue:     newCommittedResponseQueue(),
		responseStop:      make(chan struct{}),
		nestedResponses:   newNestedResponseStore(),
		contextStore:      newRequestContextStore(),
		lifecycleSpans:    make(map[string]trace.Span),
		executeSlots:      executeSlots,
		commitDrainSize:   commitDrainSize,
		learnTrace:        newPBEOLearnBatchWindowStats(cfg.Name),
		stateTrace:        newPBEOStateLockWindowStats(cfg.Name),
		responseTrace:     newPBEOResponseWindowStats(cfg.Name),
	}

	box, err := factory(BoxConfig{
		Name:                     cfg.Name,
		Peers:                    append([]string{}, cfg.Peers...),
		SendRaft:                 cfg.SendRaft,
		SendRaftBatch:            cfg.SendRaftBatch,
		TickInterval:             cfg.TickInterval,
		ElectionTick:             cfg.ElectionTick,
		HeartbeatTick:            cfg.HeartbeatTick,
		DisableFollowerElections: cfg.DisableFollowerElections,
		MaxInflightMsgs:          cfg.MaxInflightMsgs,
		MaxSizePerMsg:            cfg.MaxSizePerMsg,
		RaftSendBatchSize:        cfg.RaftSendBatchSize,
		LearnBatch:               p.LearnBatch,
		LearnBatchSize:           common.IntOrDefault(cfg.RunConfig, "pbeo_learn_batch_size", defaultRaftLearnBatchSize),
	}, p.Learn)
	if err != nil {
		return nil, err
	}
	p.box = box

	nestedDisableFollowerElections := cfg.EODisableFollowerElections || cfg.DisableFollowerElections
	nestedEO, err := eo.NewEO(eo.Config{
		Name:                     cfg.Name,
		Peers:                    append([]string{}, cfg.Peers...),
		Commit:                   p.handleCommittedNestedObservation,
		SendRaft:                 nestedSendRaft,
		SendRaftBatch:            nestedSendRaftBatch,
		BoxFactory:               cfg.NestedBoxFactory,
		TickInterval:             cfg.TickInterval,
		ElectionTick:             cfg.ElectionTick,
		HeartbeatTick:            cfg.HeartbeatTick,
		DisableFollowerElections: nestedDisableFollowerElections,
		MaxInflightMsgs:          cfg.MaxInflightMsgs,
		MaxSizePerMsg:            cfg.MaxSizePerMsg,
		RaftSendBatchSize:        cfg.EORaftSendBatchSize,
		LearnBatchSize:           cfg.EOLearnBatchSize,
		ResponseBatchSize:        cfg.EOResponseBatchSize,
		ResponseBatchTimeout:     cfg.EOResponseBatchTimeout,
	})
	if err != nil {
		box.Stop()
		return nil, err
	}
	p.nestedEO = nestedEO

	for i := 0; i < committedResponseWorkerSize; i++ {
		go p.runCommittedResponseWorker()
	}
	go p.runCommitSequencer()
	return p, nil
}

func (p *PBEO) Name() string {
	return p.name
}

func (p *PBEO) IsLeader() bool {
	return p.box.IsLeader()
}

func (p *PBEO) Leader() (string, bool) {
	return p.box.Leader()
}

func (p *PBEO) Ready() bool {
	_, stateOK := p.box.Leader()
	_, nestedOK := p.nestedEO.Leader()
	return stateOK && nestedOK
}

func (p *PBEO) Stop() {
	p.responseStopper.Do(func() {
		close(p.responseStop)
		p.responseQueue.stop()
	})
	if p.box != nil {
		p.box.Stop()
	}
	if p.nestedEO != nil {
		p.nestedEO.Stop()
	}
}

func (p *PBEO) HandleMessage(payload map[string]any) map[string]any {
	switch payloadType, _ := payload["type"].(string); payloadType {
	case MessageTypeRaft:
		return p.HandleRaftMessage(payload)
	case eo.MessageTypeRaft:
		return p.nestedEO.HandleRaftMessage(payload)
	case "response":
		if p.HandleNestedResponseObservation(payload) {
			return map[string]any{"status": "proposed_nested_observation", "request_id": payload["request_id"]}
		}
		return map[string]any{"status": "ignored_response"}
	default:
		return p.HandleRequestMessage(payload)
	}
}

func (p *PBEO) HandleRequestMessage(payload map[string]any) map[string]any {
	requestID, ok := canonicalRequestID(payload["request_id"])
	if !ok {
		return map[string]any{"status": "invalid_request", "error": "missing request_id"}
	}
	return p.HandleRequest(requestID, payload)
}

func (p *PBEO) HandleRequest(requestID string, request map[string]any) map[string]any {
	requestCopy := cloneMapAny(request)
	requestCopy["request_id"] = requestID

	if entry, ok := p.committedEntry(requestID); ok {
		go p.sendCommittedResponse(entry)
		return map[string]any{"status": "already_committed", "request_id": requestID}
	}

	if !p.box.IsLeader() {
		leader, ok := p.box.Leader()
		if !ok || leader == "" {
			return map[string]any{"status": "waiting_for_leader", "request_id": requestID}
		}
		go func() {
			_ = p.sendMessage(leader, requestCopy)
		}()
		return map[string]any{"status": "forwarded_to_leader", "request_id": requestID, "leader": leader}
	}

	if !p.tryAcceptRequest(requestID) {
		return map[string]any{"status": "duplicate_request", "request_id": requestID}
	}

	p.startRequestLifecycle(requestID, requestCopy)
	go p.executeWithAdmission(requestID, requestCopy)
	return map[string]any{"status": "accepted", "request_id": requestID, "sender": p.name}
}

func (p *PBEO) executeWithAdmission(requestID string, request map[string]any) {
	if p.executeSlots != nil {
		select {
		case p.executeSlots <- struct{}{}:
			defer func() { <-p.executeSlots }()
		case <-p.responseStop:
			p.finishRequestFailure(requestID)
			return
		}
	}
	p.executeUntilProposed(requestID, request)
}

func (p *PBEO) HandleRaftMessage(payload map[string]any) map[string]any {
	messages, err := DecodeRaftMessages(payload)
	if err != nil {
		return map[string]any{"status": "invalid_raft_message", "error": err.Error()}
	}
	for _, message := range messages {
		if err := p.box.HandleMessage(message); err != nil {
			return map[string]any{"status": "raft_step_error", "error": err.Error()}
		}
	}
	return map[string]any{"status": "raft_message_accepted", "count": len(messages)}
}

func (p *PBEO) HandleNestedResponseObservation(payload map[string]any) bool {
	if payload == nil || p.nestedEO == nil || !p.nestedEO.IsLeader() {
		return false
	}
	requestID, ok := canonicalRequestID(payload["request_id"])
	if !ok {
		return false
	}

	p.mu.Lock()
	if _, exists := p.nestedProposals[requestID]; exists {
		p.mu.Unlock()
		return true
	}
	p.nestedProposals[requestID] = struct{}{}
	p.mu.Unlock()

	proposed := cloneMapAny(payload)
	proposed["pbeo_nested_observation_committed"] = true
	if err := p.nestedEO.ProposeResponsePayload(requestID, proposed); err != nil {
		p.mu.Lock()
		delete(p.nestedProposals, requestID)
		p.mu.Unlock()
		return false
	}
	return true
}

func (p *PBEO) BufferNestedResponse(payload map[string]any) bool {
	if payload == nil {
		return false
	}
	requestIDRaw, ok := payload["parent_request_id"]
	if !ok || requestIDRaw == nil {
		requestIDRaw, ok = payload["request_id"]
	}
	if !ok || requestIDRaw == nil {
		return false
	}
	requestID, ok := canonicalRequestID(requestIDRaw)
	if !ok {
		return false
	}
	buffered := p.nestedResponses.enqueue(requestID, payload)
	if buffered {
		if request, ok := p.takeBlockedRequest(requestID); ok {
			if p.nestedResponses.promoteOne(requestID) {
				go p.executeWithAdmission(requestID, request)
			} else if p.rememberBlockedRequest(requestID, request) {
				go p.executeWithAdmission(requestID, request)
			}
		}
	}
	return buffered
}

func (p *PBEO) handleCommittedNestedObservation(committed eo.CommittedEntry) {
	_ = p.BufferNestedResponse(committed.Entry.Response)
}

func (p *PBEO) Learn(slot uint64, entry Entry) {
	p.learnBatch([]CommittedEntry{{Slot: slot, Entry: entry}})
}

func (p *PBEO) LearnBatch(entries []CommittedEntry) {
	p.learnBatch(entries)
}

func (p *PBEO) learnBatch(entries []CommittedEntry) bool {
	if len(entries) == 0 {
		return false
	}
	totalStart := time.Now()
	logStart := time.Now()
	p.mu.Lock()
	learnedAny := false
	learnedCount := 0
	duplicateCount := 0
	for _, committed := range entries {
		if _, exists := p.learnedSlots[committed.Slot]; exists {
			duplicateCount++
			continue
		}
		p.learnedSlots[committed.Slot] = struct{}{}
		p.log[committed.Slot] = cloneEntry(committed.Entry)
		if committed.Slot > p.learned {
			p.learned = committed.Slot
		}
		learnedCount++
		learnedAny = true
	}
	p.mu.Unlock()
	logDuration := time.Since(logStart)

	var processStats pbeoProcessStats
	if learnedAny {
		processStats = p.Process()
	}
	p.recordLearnBatchTrace(len(entries), learnedCount, duplicateCount, logDuration, processStats, time.Since(totalStart))
	return learnedAny
}

func (p *PBEO) recordLearnBatchTrace(entries int, learned int, duplicates int, logUpdate time.Duration, processStats pbeoProcessStats, total time.Duration) {
	p.learnTraceMu.Lock()
	defer p.learnTraceMu.Unlock()
	if p.learnTrace == nil {
		p.learnTrace = newPBEOLearnBatchWindowStats(p.name)
	}
	p.learnTrace.record(entries, learned, duplicates, logUpdate, processStats, total)
	if time.Since(p.learnTrace.start) >= raftLoopTraceWindow {
		p.learnTrace.flush("window")
	}
}

func (p *PBEO) recordStateReadTrace(wait time.Duration, hold time.Duration, total time.Duration) {
	p.stateTraceMu.Lock()
	defer p.stateTraceMu.Unlock()
	if p.stateTrace == nil {
		p.stateTrace = newPBEOStateLockWindowStats(p.name)
	}
	p.stateTrace.recordStateRead(wait, hold, total, p.executingRequests.Load())
	if time.Since(p.stateTrace.start) >= raftLoopTraceWindow {
		p.stateTrace.flush("window")
	}
}

func (p *PBEO) recordStateWriteTrace(entries int64, writes int64, writeBytes int64, wait time.Duration, hold time.Duration, total time.Duration) {
	p.stateTraceMu.Lock()
	defer p.stateTraceMu.Unlock()
	if p.stateTrace == nil {
		p.stateTrace = newPBEOStateLockWindowStats(p.name)
	}
	p.stateTrace.recordStateWrite(entries, writes, writeBytes, wait, hold, total)
	if time.Since(p.stateTrace.start) >= raftLoopTraceWindow {
		p.stateTrace.flush("window")
	}
}

func (p *PBEO) recordResponseEnqueueTrace(depth int, duration time.Duration) {
	p.responseTraceMu.Lock()
	defer p.responseTraceMu.Unlock()
	if p.responseTrace == nil {
		p.responseTrace = newPBEOResponseWindowStats(p.name)
	}
	p.responseTrace.recordEnqueue(depth, duration)
	if time.Since(p.responseTrace.start) >= raftLoopTraceWindow {
		p.responseTrace.flush("window")
	}
}

func (p *PBEO) recordResponseSendTrace(depth int, clients int, queueWait time.Duration, sendDuration time.Duration) {
	p.responseTraceMu.Lock()
	defer p.responseTraceMu.Unlock()
	if p.responseTrace == nil {
		p.responseTrace = newPBEOResponseWindowStats(p.name)
	}
	p.responseTrace.recordSend(depth, clients, queueWait, sendDuration)
	if time.Since(p.responseTrace.start) >= raftLoopTraceWindow {
		p.responseTrace.flush("window")
	}
}

func (p *PBEO) Process() pbeoProcessStats {
	start := time.Now()
	lockStart := time.Now()
	p.processMu.Lock()
	lockDuration := time.Since(lockStart)
	defer p.processMu.Unlock()

	dequeueStart := time.Now()
	committed := p.dequeueCommittableEntries()
	dequeueDuration := time.Since(dequeueStart)
	stats := pbeoProcessStats{
		entries:          int64(len(committed)),
		processLockNanos: lockDuration.Nanoseconds(),
		dequeueNanos:     dequeueDuration.Nanoseconds(),
	}
	if len(committed) > 0 {
		applyStart := time.Now()
		applyStats := p.applyCommittedEntries(committed)
		stats.applyNanos += time.Since(applyStart).Nanoseconds()
		stats.apply.entries += applyStats.entries
		stats.apply.writes += applyStats.writes
		stats.apply.writeBytes += applyStats.writeBytes
		stats.apply.responsesEnqueued += applyStats.responsesEnqueued
		stats.apply.stateWriteNanos += applyStats.stateWriteNanos
		stats.apply.stateWriteWaitNanos += applyStats.stateWriteWaitNanos
		stats.apply.stateWriteHoldNanos += applyStats.stateWriteHoldNanos
		stats.apply.clearStateNanos += applyStats.clearStateNanos
		stats.apply.responseEnqueueNanos += applyStats.responseEnqueueNanos
	}
	stats.processNanos = time.Since(start).Nanoseconds()
	return stats
}

func (p *PBEO) LearnedIndex() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.learned
}

func (p *PBEO) ProcessedIndex() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.processed
}

func (p *PBEO) ReadKV(key string) string {
	return p.readKV(key, false)
}

func (p *PBEO) readKV(key string, traceRead bool) string {
	totalStart := time.Now()
	waitStart := time.Now()
	p.stateMu.RLock()
	waitDuration := time.Since(waitStart)
	holdStart := time.Now()
	value := p.kv[key]
	holdDuration := time.Since(holdStart)
	p.stateMu.RUnlock()
	if traceRead {
		p.recordStateReadTrace(waitDuration, holdDuration, time.Since(totalStart))
	}
	return value
}

func (p *PBEO) StateSnapshot() map[string]string {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return copyStringMap(p.kv)
}

func (p *PBEO) executeUntilProposed(requestID string, request map[string]any) {
	defer func() {
		if recovered := recover(); recovered != nil {
			p.finishRequestFailure(requestID)
		}
	}()

	if _, ok := p.committedEntry(requestID); ok {
		return
	}

	p.executingRequests.Add(1)
	defer p.executingRequests.Add(-1)

	tx := newTxn(p, requestID)
	response := p.execute(tx, cloneMapAny(request))
	if response == nil {
		response = map[string]any{}
	}
	if _, ok := response["request_id"]; !ok {
		response["request_id"] = requestID
	}
	if common.GetString(response, "status") == "blocked_for_nested_response" {
		if p.rememberBlockedRequest(requestID, request) {
			go p.executeWithAdmission(requestID, request)
		}
		return
	}

	submission := candidateSubmission{
		requestID: requestID,
		response:  cloneMapAny(response),
		writes:    tx.Writes(),
	}

	p.commitCh <- submission
}

func (p *PBEO) runCommitSequencer() {
	for submission := range p.commitCh {
		p.processCommitSubmission(submission)
	drain:
		for i := 1; i < p.commitDrainSize; i++ {
			select {
			case submission, ok := <-p.commitCh:
				if !ok {
					return
				}
				p.processCommitSubmission(submission)
			default:
				break drain
			}
		}
	}
}

func (p *PBEO) processCommitSubmission(submission candidateSubmission) {
	if _, ok := p.committedEntry(submission.requestID); ok {
		return
	}

	entry := Entry{
		RequestID: submission.requestID,
		Response:  cloneMapAny(submission.response),
		Writes:    copyStringMap(submission.writes),
	}
	err := p.proposeCommitEntry(entry)
	if err != nil {
		p.finishRequestFailure(submission.requestID)
		return
	}
}

func (p *PBEO) proposeCommitEntry(entry Entry) error {
	if box, ok := p.box.(asyncConsensusBox); ok {
		return box.ProposeAsync(entry)
	}
	return p.box.Propose(entry)
}

func stringMapBytes(values map[string]string) int {
	total := 0
	for key, value := range values {
		total += len(key) + len(value)
	}
	return total
}

func (p *PBEO) dequeueCommittableEntries() []CommittedEntry {
	p.mu.Lock()
	defer p.mu.Unlock()

	entries := make([]CommittedEntry, 0)
	for {
		nextSlot := p.processed + 1
		if nextSlot > p.learned {
			return entries
		}
		entry, ok := p.log[nextSlot]
		if !ok {
			return entries
		}
		p.processed = nextSlot
		if _, duplicate := p.committedRequests[entry.RequestID]; duplicate {
			continue
		}
		p.committedRequests[entry.RequestID] = cloneEntry(entry)
		delete(p.blockedRequests, entry.RequestID)
		delete(p.requestAttempts, entry.RequestID)
		entries = append(entries, CommittedEntry{Slot: nextSlot, Entry: cloneEntry(entry)})
	}
}

func (p *PBEO) applyCommittedEntries(committed []CommittedEntry) pbeoApplyStats {
	stats := pbeoApplyStats{
		entries: int64(len(committed)),
	}
	if len(committed) == 0 {
		return stats
	}

	type pendingApply struct {
		entry Entry
	}

	pending := make([]pendingApply, 0, len(committed))
	for _, committedEntry := range committed {
		entry := committedEntry.Entry
		writeBytes := stringMapBytes(entry.Writes)
		stats.writes += int64(len(entry.Writes))
		stats.writeBytes += int64(writeBytes)
		pending = append(pending, pendingApply{
			entry: entry,
		})
	}

	stateStart := time.Now()
	waitStart := time.Now()
	p.stateMu.Lock()
	waitDuration := time.Since(waitStart)
	holdStart := time.Now()
	for _, item := range pending {
		for key, value := range item.entry.Writes {
			p.kv[key] = value
		}
	}
	holdDuration := time.Since(holdStart)
	p.stateMu.Unlock()
	stats.stateWriteNanos = time.Since(stateStart).Nanoseconds()
	stats.stateWriteWaitNanos = waitDuration.Nanoseconds()
	stats.stateWriteHoldNanos = holdDuration.Nanoseconds()
	p.recordStateWriteTrace(stats.entries, stats.writes, stats.writeBytes, waitDuration, holdDuration, time.Duration(stats.stateWriteNanos))

	for _, item := range pending {
		entry := item.entry

		clearStart := time.Now()
		p.contextStore.clear(entry.RequestID)
		p.nestedResponses.clear(entry.RequestID)
		stats.clearStateNanos += time.Since(clearStart).Nanoseconds()

		enqueueStart := time.Now()
		if p.enqueueCommittedResponse(entry) {
			stats.responseEnqueueNanos += time.Since(enqueueStart).Nanoseconds()
			stats.responsesEnqueued++
			continue
		}
		stats.responseEnqueueNanos += time.Since(enqueueStart).Nanoseconds()
		p.endRequestLifecycle(entry.RequestID, "stopped_before_response")
	}
	return stats
}

func (p *PBEO) enqueueCommittedResponse(entry Entry) bool {
	start := time.Now()
	task := committedResponseTask{
		entry:    cloneEntry(entry),
		enqueued: time.Now(),
	}
	depth, ok := p.responseQueue.enqueue(task)
	if ok {
		p.recordResponseEnqueueTrace(depth, time.Since(start))
	}
	return ok
}

func (p *PBEO) runCommittedResponseWorker() {
	for {
		task, queueDepth, ok := p.responseQueue.dequeue()
		if !ok {
			return
		}
		queueWait := time.Since(task.enqueued)
		sendStart := time.Now()
		clients := p.sendCommittedResponse(task.entry)
		sendDuration := time.Since(sendStart)
		p.recordResponseSendTrace(queueDepth, clients, queueWait, sendDuration)
	}
}

func (p *PBEO) sendCommittedResponse(entry Entry) int {
	if len(p.clients) == 0 {
		p.endRequestLifecycle(entry.RequestID, "no_clients", attribute.Int("pbeo.client_count", 0))
		return 0
	}
	response := cloneMapAny(entry.Response)
	requestID := entry.RequestID
	if responseID, ok := canonicalRequestID(response["request_id"]); ok {
		requestID = responseID
	}
	message := map[string]any{
		"type":                   "response",
		"request_id":             requestID,
		"response":               response,
		"sender":                 p.name,
		"pbeo_committed":         true,
		"shim_quorum_aggregated": true,
	}
	if parentRequestID, ok := response["parent_request_id"]; ok && parentRequestID != nil {
		message["parent_request_id"] = parentRequestID
	}

	for _, client := range p.clients {
		_ = p.sendMessage(client, cloneMapAny(message))
	}
	p.endRequestLifecycle(
		entry.RequestID,
		"sent_to_clients",
		attribute.Int("pbeo.client_count", len(p.clients)),
	)
	return len(p.clients)
}

func (p *PBEO) startRequestLifecycle(requestID string, request map[string]any) {
	if requestID == "" || !telemetry.DetailedSpansEnabled() {
		return
	}
	_, span := telemetry.StartSpanFromPayload(
		request,
		"pbeo.request_to_client_response",
		append(
			telemetry.AttrsFromPayload(request),
			attribute.String("node.name", p.name),
			attribute.String("request.id", requestID),
			attribute.Int("pbeo.client_count", len(p.clients)),
		)...,
	)
	p.mu.Lock()
	p.lifecycleSpans[requestID] = span
	p.mu.Unlock()
}

func (p *PBEO) endRequestLifecycle(requestID string, result string, attrs ...attribute.KeyValue) {
	if requestID == "" || !telemetry.DetailedSpansEnabled() {
		return
	}
	p.mu.Lock()
	span := p.lifecycleSpans[requestID]
	delete(p.lifecycleSpans, requestID)
	p.mu.Unlock()
	if span == nil {
		return
	}
	attrs = append(attrs, attribute.String("pbeo.request_lifecycle_result", result))
	span.SetAttributes(attrs...)
	span.End()
}

func (p *PBEO) sendMessage(peer string, payload map[string]any) error {
	if p.send == nil {
		return fmt.Errorf("pbeo sender is not configured")
	}
	return p.send(peer, payload)
}

func (p *PBEO) tryAcceptRequest(requestID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, committed := p.committedRequests[requestID]; committed {
		return false
	}
	if _, exists := p.requestAttempts[requestID]; exists {
		return false
	}
	p.requestAttempts[requestID] = struct{}{}
	return true
}

func (p *PBEO) rememberBlockedRequest(requestID string, request map[string]any) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, committed := p.committedRequests[requestID]; committed {
		return false
	}
	if p.nestedResponses.promoteOne(requestID) {
		return true
	}
	p.blockedRequests[requestID] = cloneMapAny(request)
	return false
}

func (p *PBEO) takeBlockedRequest(requestID string) (map[string]any, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	request, ok := p.blockedRequests[requestID]
	if !ok {
		return nil, false
	}
	delete(p.blockedRequests, requestID)
	return cloneMapAny(request), true
}

func (p *PBEO) finishRequestFailure(requestID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.requestAttempts, requestID)
	delete(p.blockedRequests, requestID)
	if telemetry.DetailedSpansEnabled() {
		span := p.lifecycleSpans[requestID]
		delete(p.lifecycleSpans, requestID)
		if span != nil {
			span.SetAttributes(attribute.String("pbeo.request_lifecycle_result", "failed_before_commit"))
			span.End()
		}
	}
}

func (p *PBEO) committedEntry(requestID string) (Entry, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.committedRequests[requestID]
	return cloneEntry(entry), ok
}

func cloneEntry(entry Entry) Entry {
	return Entry{
		RequestID: entry.RequestID,
		Response:  cloneMapAny(entry.Response),
		Writes:    copyStringMap(entry.Writes),
	}
}

func cloneRunConfig(src map[string]any) map[string]any {
	if src == nil {
		return map[string]any{}
	}
	return cloneMapAny(src)
}
