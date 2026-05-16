package pbeo

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"aegean/eo"
	"aegean/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type candidateSubmission struct {
	requestID string
	response  map[string]any
	writes    map[string]string
	result    chan candidateResult
	queueSpan trace.Span
	enqueued  time.Time
}

type candidateResult struct {
	err error
}

type asyncConsensusBox interface {
	ProposeAsync(entry Entry) error
}

type committedResponseTask struct {
	entry    Entry
	enqueued time.Time
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
	nestedProposals   map[string]struct{}

	stateMu           sync.RWMutex
	kv                map[string]string
	executingRequests atomic.Int64
	commitSequence    atomic.Uint64

	commitCh        chan candidateSubmission
	responseCh      chan committedResponseTask
	responseStop    chan struct{}
	responseStopper sync.Once
	nestedResponses *nestedResponseStore
	contextStore    *requestContextStore
	lifecycleSpans  map[string]trace.Span
}

const (
	committedResponseQueueSize  = 4096
	committedResponseWorkerSize = 16
)

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
		nestedProposals:   make(map[string]struct{}),
		kv:                initial,
		commitCh:          make(chan candidateSubmission, 1024),
		responseCh:        make(chan committedResponseTask, committedResponseQueueSize),
		responseStop:      make(chan struct{}),
		nestedResponses:   newNestedResponseStore(),
		contextStore:      newRequestContextStore(),
		lifecycleSpans:    make(map[string]trace.Span),
	}

	box, err := factory(BoxConfig{
		Name:              cfg.Name,
		Peers:             append([]string{}, cfg.Peers...),
		SendRaft:          cfg.SendRaft,
		SendRaftBatch:     cfg.SendRaftBatch,
		TickInterval:      cfg.TickInterval,
		ElectionTick:      cfg.ElectionTick,
		HeartbeatTick:     cfg.HeartbeatTick,
		MaxInflightMsgs:   cfg.MaxInflightMsgs,
		MaxSizePerMsg:     cfg.MaxSizePerMsg,
		RaftSendBatchSize: cfg.RaftSendBatchSize,
	}, p.Learn)
	if err != nil {
		return nil, err
	}
	p.box = box

	nestedEO, err := eo.NewEO(eo.Config{
		Name:              cfg.Name,
		Peers:             append([]string{}, cfg.Peers...),
		Commit:            p.handleCommittedNestedObservation,
		SendRaft:          nestedSendRaft,
		SendRaftBatch:     nestedSendRaftBatch,
		BoxFactory:        cfg.NestedBoxFactory,
		TickInterval:      cfg.TickInterval,
		ElectionTick:      cfg.ElectionTick,
		HeartbeatTick:     cfg.HeartbeatTick,
		MaxInflightMsgs:   cfg.MaxInflightMsgs,
		MaxSizePerMsg:     cfg.MaxSizePerMsg,
		RaftSendBatchSize: cfg.EORaftSendBatchSize,
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
	go p.executeUntilProposed(requestID, requestCopy)
	return map[string]any{"status": "accepted", "request_id": requestID, "sender": p.name}
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
	return p.nestedResponses.enqueue(requestID, payload)
}

func (p *PBEO) handleCommittedNestedObservation(committed eo.CommittedEntry) {
	_ = p.BufferNestedResponse(committed.Entry.Response)
}

func (p *PBEO) Learn(slot uint64, entry Entry) {
	_, learnSpan := telemetry.StartSpanFromPayload(
		entry.Response,
		"pbeo.learn_to_client_response",
		append(
			p.entryAttrs(entry),
			attribute.Int64("pbeo.commit_slot", int64(slot)),
		)...,
	)
	p.mu.Lock()
	if _, exists := p.learnedSlots[slot]; exists {
		p.mu.Unlock()
		endPBEOTrace(learnSpan, "duplicate_slot")
		return
	}
	p.learnedSlots[slot] = struct{}{}
	p.log[slot] = cloneEntry(entry)
	if slot > p.learned {
		p.learned = slot
	}
	p.mu.Unlock()

	p.Process()
	endPBEOTrace(learnSpan, "processed")
}

func (p *PBEO) Process() {
	p.processMu.Lock()
	defer p.processMu.Unlock()

	committed := p.dequeueCommittableEntries()
	for _, entry := range committed {
		p.applyCommittedEntry(entry)
	}
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
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.kv[key]
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

	_, executeSpan := telemetry.StartSpanFromPayload(
		request,
		"pbeo.request_execute",
		append(
			telemetry.AttrsFromPayload(request),
			attribute.String("node.name", p.name),
			attribute.String("request.id", requestID),
			attribute.Int64("pbeo.executing_requests_on_start", p.executingRequests.Add(1)),
		)...,
	)
	snapshot := p.snapshot()
	tx := newTxn(p, requestID, snapshot)
	response := p.execute(tx, cloneMapAny(request))
	if response == nil {
		response = map[string]any{}
	}
	if _, ok := response["request_id"]; !ok {
		response["request_id"] = requestID
	}
	executeSpan.SetAttributes(
		attribute.Int("pbeo.write_count", len(tx.Writes())),
		attribute.Int("pbeo.write_bytes", stringMapBytes(tx.Writes())),
		attribute.Int64("pbeo.executing_requests_on_end", p.executingRequests.Add(-1)),
	)
	executeSpan.End()

	resultCh := make(chan candidateResult, 1)
	_, queueSpan := telemetry.StartSpanFromPayload(
		response,
		"pbeo.commit_queue_wait",
		append(
			telemetry.AttrsFromPayload(response),
			attribute.String("node.name", p.name),
			attribute.String("request.id", requestID),
			attribute.Int("pbeo.commit_queue_depth_on_enqueue", len(p.commitCh)),
			attribute.Int("pbeo.write_count", len(tx.Writes())),
			attribute.Int("pbeo.write_bytes", stringMapBytes(tx.Writes())),
		)...,
	)
	submission := candidateSubmission{
		requestID: requestID,
		response:  cloneMapAny(response),
		writes:    tx.Writes(),
		result:    resultCh,
		queueSpan: queueSpan,
		enqueued:  time.Now(),
	}

	p.commitCh <- submission

	result := <-resultCh
	if result.err != nil {
		p.finishRequestFailure(requestID)
	}
}

func (p *PBEO) runCommitSequencer() {
	for submission := range p.commitCh {
		seq := p.commitSequence.Add(1)
		queueDepthOnDequeue := len(p.commitCh)
		queueWait := time.Since(submission.enqueued)
		_, iterationSpan := telemetry.StartSpanFromPayload(
			submission.response,
			"pbeo.commit_sequencer_iteration",
			append(
				telemetry.AttrsFromPayload(submission.response),
				attribute.String("node.name", p.name),
				attribute.String("request.id", submission.requestID),
				attribute.Int("pbeo.commit_queue_depth_on_dequeue", queueDepthOnDequeue),
				attribute.Int64("pbeo.commit_sequence", int64(seq)),
				attribute.Int64("pbeo.commit_queue_wait_us", queueWait.Microseconds()),
			)...,
		)
		if submission.queueSpan != nil {
			submission.queueSpan.SetAttributes(
				attribute.Int("pbeo.commit_queue_depth_on_dequeue", queueDepthOnDequeue),
				attribute.Int64("pbeo.commit_sequence", int64(seq)),
				attribute.Int64("pbeo.commit_queue_wait_us", queueWait.Microseconds()),
			)
			submission.queueSpan.End()
		}
		if _, ok := p.committedEntry(submission.requestID); ok {
			submission.result <- candidateResult{}
			endPBEOTrace(iterationSpan, "already_committed")
			continue
		}

		entry := Entry{
			RequestID: submission.requestID,
			Response:  cloneMapAny(submission.response),
			Writes:    copyStringMap(submission.writes),
		}
		proposeStart := time.Now()
		err := p.proposeCommitEntry(entry)
		if err != nil {
			iterationSpan.SetAttributes(attribute.Int64("pbeo.commit_sequencer_propose_us", time.Since(proposeStart).Microseconds()))
			iterationSpan.RecordError(err)
			submission.result <- candidateResult{err: err}
			endPBEOTrace(iterationSpan, "propose_error")
			continue
		}
		iterationSpan.SetAttributes(attribute.Int64("pbeo.commit_sequencer_propose_us", time.Since(proposeStart).Microseconds()))
		submission.result <- candidateResult{}
		endPBEOTrace(iterationSpan, "proposed")
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

func (p *PBEO) snapshot() stateSnapshot {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return stateSnapshot{
		kv: copyStringMap(p.kv),
	}
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
		delete(p.requestAttempts, entry.RequestID)
		entries = append(entries, CommittedEntry{Slot: nextSlot, Entry: cloneEntry(entry)})
	}
}

func (p *PBEO) applyCommittedEntry(committed CommittedEntry) {
	entry := committed.Entry
	_, span := telemetry.StartSpanFromPayload(
		entry.Response,
		"pbeo.apply_committed_entry",
		append(
			p.entryAttrs(entry),
			attribute.Int64("pbeo.commit_slot", int64(committed.Slot)),
			attribute.Int("pbeo.write_count", len(entry.Writes)),
			attribute.Int("pbeo.write_bytes", stringMapBytes(entry.Writes)),
			attribute.Int("pbeo.client_count", len(p.clients)),
		)...,
	)
	p.stateMu.Lock()
	for key, value := range entry.Writes {
		p.kv[key] = value
	}
	p.stateMu.Unlock()

	p.contextStore.clear(entry.RequestID)
	p.nestedResponses.clear(entry.RequestID)

	if p.enqueueCommittedResponse(entry) {
		span.SetAttributes(attribute.Int("pbeo.response_queue_depth_on_enqueue", len(p.responseCh)))
		endPBEOTrace(span, "response_enqueued")
		return
	}
	p.endRequestLifecycle(entry.RequestID, "stopped_before_response")
	endPBEOTrace(span, "response_enqueue_stopped")
}

func (p *PBEO) enqueueCommittedResponse(entry Entry) bool {
	task := committedResponseTask{
		entry:    cloneEntry(entry),
		enqueued: time.Now(),
	}
	select {
	case <-p.responseStop:
		return false
	case p.responseCh <- task:
		return true
	}
}

func (p *PBEO) runCommittedResponseWorker() {
	for {
		select {
		case <-p.responseStop:
			return
		case task := <-p.responseCh:
			_, span := telemetry.StartSpanFromPayload(
				task.entry.Response,
				"pbeo.response_worker_iteration",
				append(
					p.entryAttrs(task.entry),
					attribute.Int("pbeo.response_queue_depth_on_dequeue", len(p.responseCh)),
					attribute.Int64("pbeo.response_queue_wait_us", time.Since(task.enqueued).Microseconds()),
				)...,
			)
			p.sendCommittedResponse(task.entry)
			endPBEOTrace(span, "sent_to_clients")
		}
	}
}

func (p *PBEO) sendCommittedResponse(entry Entry) {
	if len(p.clients) == 0 {
		p.endRequestLifecycle(entry.RequestID, "no_clients", attribute.Int("pbeo.client_count", 0))
		return
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
}

func (p *PBEO) entryAttrs(entry Entry) []attribute.KeyValue {
	attrs := append([]attribute.KeyValue{}, telemetry.AttrsFromPayload(entry.Response)...)
	attrs = append(attrs, attribute.String("node.name", p.name))
	if entry.RequestID != "" {
		attrs = append(attrs, attribute.String("request.id", entry.RequestID))
	}
	return attrs
}

func (p *PBEO) startRequestLifecycle(requestID string, request map[string]any) {
	if requestID == "" {
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
	if requestID == "" {
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

func (p *PBEO) finishRequestFailure(requestID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.requestAttempts, requestID)
	span := p.lifecycleSpans[requestID]
	delete(p.lifecycleSpans, requestID)
	if span != nil {
		span.SetAttributes(attribute.String("pbeo.request_lifecycle_result", "failed_before_commit"))
		span.End()
	}
}

func endPBEOTrace(span trace.Span, event string) {
	if span == nil {
		return
	}
	span.AddEvent(event)
	span.End()
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
