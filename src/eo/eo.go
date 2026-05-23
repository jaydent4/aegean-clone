package eo

import (
	"fmt"
	"log"
	"sync"
	"time"

	"aegean/common"
)

const defaultResponseBatchQueueSize = 4096

type EO struct {
	name    string
	execute ExecuteFunc
	commit  CommitFunc
	forward ForwardFunc
	box     ConsensusBox

	responseBatchSize     int
	responseBatchTimeout  time.Duration
	responseBatchQueue    chan responseProposal
	responseBatchStop     chan struct{}
	responseBatchDone     chan struct{}
	responseBatchStopOnce sync.Once

	processMu         sync.Mutex
	mu                sync.Mutex
	log               map[uint64]Entry
	learned           uint64
	processed         uint64
	requestAttempts   map[string]struct{}
	learnedSlots      map[uint64]struct{}
	committedRequests map[string]struct{}
}

type asyncConsensusBox interface {
	ProposeAsync(entry Entry) error
}

type responseProposal struct {
	item EntryItem
}

func NewEO(cfg Config) (*EO, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("eo requires a node name")
	}
	if len(cfg.Peers) == 0 {
		return nil, fmt.Errorf("eo requires at least one peer")
	}
	factory := cfg.BoxFactory
	if factory == nil {
		if cfg.SendRaft == nil {
			return nil, fmt.Errorf("eo requires a raft sender when using the default consensus box")
		}
		factory = newRaftConsensusBox
	}

	commit := cfg.Commit
	if commit == nil {
		commit = cfg.Apply
	}

	e := &EO{
		name:              cfg.Name,
		execute:           cfg.Execute,
		commit:            commit,
		forward:           cfg.Forward,
		log:               make(map[uint64]Entry),
		requestAttempts:   make(map[string]struct{}),
		learnedSlots:      make(map[uint64]struct{}),
		committedRequests: make(map[string]struct{}),
	}
	if cfg.ResponseBatchSize > 1 {
		e.responseBatchSize = cfg.ResponseBatchSize
		e.responseBatchTimeout = cfg.ResponseBatchTimeout
		e.responseBatchQueue = make(chan responseProposal, defaultResponseBatchQueueSize)
		e.responseBatchStop = make(chan struct{})
		e.responseBatchDone = make(chan struct{})
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
		LearnBatch:               e.LearnBatch,
		LearnBatchSize:           cfg.LearnBatchSize,
	}, e.Learn)
	if err != nil {
		return nil, err
	}
	e.box = box
	if e.responseBatchQueue != nil {
		go e.runResponseBatcher()
	}
	return e, nil
}

func (e *EO) Name() string {
	return e.name
}

func (e *EO) IsLeader() bool {
	return e.box.IsLeader()
}

// IsPrimary is kept as a compatibility alias for older callers.
func (e *EO) IsPrimary() bool {
	return e.IsLeader()
}

func (e *EO) Leader() (string, bool) {
	return e.box.Leader()
}

// Primary is kept as a compatibility alias for older callers.
func (e *EO) Primary() (string, bool) {
	return e.Leader()
}

func (e *EO) Stop() {
	e.stopResponseBatcher()
	if e.box != nil {
		e.box.Stop()
	}
}

func (e *EO) ProposeResponsePayload(requestID string, payload map[string]any) error {
	if requestID == "" {
		return fmt.Errorf("eo propose requires a request_id")
	}
	item := EntryItem{
		RequestID: requestID,
		Response:  cloneMap(payload),
	}
	if e.responseBatchQueue != nil {
		proposal := responseProposal{item: item}
		select {
		case <-e.responseBatchStop:
			return fmt.Errorf("eo stopped")
		case e.responseBatchQueue <- proposal:
		}
		return nil
	}
	return e.proposeEntry(entryFromItem(item))
}

func (e *EO) proposeEntry(entry Entry) error {
	if box, ok := e.box.(asyncConsensusBox); ok {
		return box.ProposeAsync(entry)
	}
	return e.box.Propose(entry)
}

func (e *EO) runResponseBatcher() {
	defer close(e.responseBatchDone)

	batch := make([]responseProposal, 0, e.responseBatchSize)

	for {
		select {
		case <-e.responseBatchStop:
			e.flushResponseProposalQueue(batch)
			return
		case proposal := <-e.responseBatchQueue:
			batch = append(batch[:0], proposal)
			if e.responseBatchTimeout > 0 {
				var stopped bool
				batch, stopped = e.waitForResponseBatch(batch)
				if stopped {
					return
				}
			}
		drain:
			for len(batch) < e.responseBatchSize {
				select {
				case next := <-e.responseBatchQueue:
					batch = append(batch, next)
				default:
					break drain
				}
			}
			e.proposeResponseBatch(batch)
		}
	}
}

func (e *EO) waitForResponseBatch(batch []responseProposal) ([]responseProposal, bool) {
	timer := time.NewTimer(e.responseBatchTimeout)
	defer timer.Stop()
	for len(batch) < e.responseBatchSize {
		select {
		case <-e.responseBatchStop:
			e.proposeResponseBatch(batch)
			e.flushResponseProposalQueue(batch)
			return batch, true
		case next := <-e.responseBatchQueue:
			batch = append(batch, next)
		case <-timer.C:
			return batch, false
		}
	}
	return batch, false
}

func (e *EO) flushResponseProposalQueue(batch []responseProposal) {
	for {
		batch = batch[:0]
	drain:
		for len(batch) < e.responseBatchSize {
			select {
			case proposal := <-e.responseBatchQueue:
				batch = append(batch, proposal)
			default:
				break drain
			}
		}
		if len(batch) == 0 {
			return
		}
		e.proposeResponseBatch(batch)
	}
}

func (e *EO) proposeResponseBatch(batch []responseProposal) {
	entry := entryFromResponseProposals(batch)
	err := e.proposeEntry(entry)
	if err != nil {
		log.Printf("%s: warning: EO response batch proposal failed responses=%d error=%v", e.name, len(batch), err)
	}
}

func (e *EO) stopResponseBatcher() {
	if e.responseBatchStop == nil {
		return
	}
	e.responseBatchStopOnce.Do(func() {
		close(e.responseBatchStop)
		<-e.responseBatchDone
	})
}

func (e *EO) HandleMessage(payload map[string]any) map[string]any {
	if common.GetString(payload, "type") == MessageTypeRaft {
		return e.HandleRaftMessage(payload)
	}
	return e.HandleRequestMessage(payload)
}

func (e *EO) HandleRequestMessage(payload map[string]any) map[string]any {
	requestID, ok := canonicalRequestID(payload["request_id"])
	if !ok {
		return map[string]any{"status": "invalid_request", "error": "missing request_id"}
	}
	return e.HandleRequest(requestID, payload)
}

func (e *EO) HandleRequest(requestID string, request map[string]any) map[string]any {
	requestCopy := cloneMap(request)
	requestCopy["request_id"] = requestID

	if !e.box.IsLeader() {
		leader, ok := e.box.Leader()
		if !ok || leader == "" {
			return map[string]any{
				"status":     "waiting_for_leader",
				"request_id": requestID,
			}
		}
		if e.forward == nil {
			return map[string]any{
				"status":     "not_leader",
				"request_id": requestID,
				"leader":     leader,
			}
		}
		if err := e.forward(leader, requestID, requestCopy); err != nil {
			return map[string]any{
				"status":     "forward_error",
				"request_id": requestID,
				"leader":     leader,
				"error":      err.Error(),
			}
		}
		return map[string]any{
			"status":     "forwarded_to_leader",
			"request_id": requestID,
			"leader":     leader,
		}
	}

	if e.execute == nil {
		return map[string]any{
			"status":     "execution_error",
			"request_id": requestID,
			"error":      "eo execute callback is not configured",
		}
	}

	if !e.tryAcceptRequest(requestID) {
		return map[string]any{
			"status":     "duplicate_request",
			"request_id": requestID,
		}
	}

	response, err := e.execute(requestID, requestCopy)
	if err != nil {
		e.finishRequestFailure(requestID)
		return map[string]any{
			"status":     "execution_error",
			"request_id": requestID,
			"error":      err.Error(),
		}
	}
	if response == nil {
		response = map[string]any{}
	}

	entry := Entry{
		RequestID: requestID,
		Response:  response,
	}
	if err := e.box.Propose(entry); err != nil {
		e.finishRequestFailure(requestID)
		return map[string]any{
			"status":     "proposal_error",
			"request_id": requestID,
			"error":      err.Error(),
		}
	}

	return map[string]any{
		"status":     "proposed",
		"request_id": requestID,
	}
}

func (e *EO) HandleRaftMessage(payload map[string]any) map[string]any {
	if _, ok := payload[raftMessageKey]; ok {
		message, err := DecodeRaftMessage(payload)
		if err != nil {
			return map[string]any{
				"status": "invalid_raft_message",
				"error":  err.Error(),
			}
		}
		if err := e.box.HandleMessage(message); err != nil {
			return map[string]any{
				"status": "raft_step_error",
				"error":  err.Error(),
			}
		}
		return map[string]any{"status": "raft_message_accepted"}
	}
	messages, err := DecodeRaftMessages(payload)
	if err != nil {
		return map[string]any{
			"status": "invalid_raft_message",
			"error":  err.Error(),
		}
	}
	for _, message := range messages {
		if err := e.box.HandleMessage(message); err != nil {
			return map[string]any{
				"status": "raft_step_error",
				"error":  err.Error(),
			}
		}
	}
	return map[string]any{"status": "raft_message_accepted", "count": len(messages)}
}

// Learn is the callback that the consensus box invokes when a slot is learned.
func (e *EO) Learn(slot uint64, entry Entry) {
	e.learnBatch([]CommittedEntry{{Slot: slot, Entry: entry}})
}

func (e *EO) LearnBatch(entries []CommittedEntry) {
	e.learnBatch(entries)
}

func (e *EO) learnBatch(entries []CommittedEntry) bool {
	if len(entries) == 0 {
		return false
	}

	e.mu.Lock()
	learnedAny := false
	for _, committed := range entries {
		if _, exists := e.learnedSlots[committed.Slot]; exists {
			continue
		}
		e.learnedSlots[committed.Slot] = struct{}{}
		e.log[committed.Slot] = cloneEntry(committed.Entry)
		if committed.Slot > e.learned {
			e.learned = committed.Slot
		}
		learnedAny = true
	}
	e.mu.Unlock()

	if learnedAny {
		e.Process()
	}
	return learnedAny
}

// OnCommit is kept as a compatibility alias for older consensus-box callbacks.
func (e *EO) OnCommit(slot uint64, entry Entry) {
	e.Learn(slot, entry)
}

// Process best-effort processes every contiguous learned slot and commits only
// the first learned occurrence of each request id to the executor.
func (e *EO) Process() {
	e.processMu.Lock()
	defer e.processMu.Unlock()

	committedEntries := e.dequeueCommittableEntries()
	if e.commit == nil {
		return
	}
	for _, entry := range committedEntries {
		e.commit(entry)
	}
}

// TryApply is kept as a compatibility alias for older callers.
func (e *EO) TryApply() {
	e.Process()
}

func (e *EO) LearnedIndex() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.learned
}

// CommitIndex is kept as a compatibility alias for older callers.
func (e *EO) CommitIndex() uint64 {
	return e.LearnedIndex()
}

func (e *EO) ProcessedIndex() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.processed
}

// AppliedIndex is kept as a compatibility alias for older callers.
func (e *EO) AppliedIndex() uint64 {
	return e.ProcessedIndex()
}

func (e *EO) Entry(slot uint64) (Entry, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	entry, ok := e.log[slot]
	return entry, ok
}

func (e *EO) tryAcceptRequest(requestID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.requestAttempts[requestID]; exists {
		return false
	}
	e.requestAttempts[requestID] = struct{}{}
	return true
}

func (e *EO) finishRequestFailure(requestID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.requestAttempts, requestID)
}

func (e *EO) dequeueCommittableEntries() []CommittedEntry {
	e.mu.Lock()
	defer e.mu.Unlock()

	committedEntries := make([]CommittedEntry, 0)
	for {
		nextSlot := e.processed + 1
		if nextSlot > e.learned {
			return committedEntries
		}
		entry, ok := e.log[nextSlot]
		if !ok {
			return committedEntries
		}
		e.processed = nextSlot
		for _, item := range entryItems(entry) {
			if item.RequestID == "" {
				continue
			}
			if _, duplicate := e.committedRequests[item.RequestID]; duplicate {
				continue
			}
			e.committedRequests[item.RequestID] = struct{}{}
			committedEntries = append(committedEntries, CommittedEntry{
				Slot:  nextSlot,
				Entry: entryFromItem(item),
			})
		}
	}
}

func entryFromItem(item EntryItem) Entry {
	return Entry{
		RequestID: item.RequestID,
		Response:  item.Response,
	}
}

func entryFromResponseProposals(proposals []responseProposal) Entry {
	if len(proposals) == 0 {
		return Entry{}
	}
	if len(proposals) == 1 {
		return entryFromItem(proposals[0].item)
	}
	batch := make([]EntryItem, len(proposals))
	for i, proposal := range proposals {
		batch[i] = proposal.item
	}
	return Entry{
		RequestID: batch[0].RequestID,
		Batch:     batch,
	}
}

func entryItems(entry Entry) []EntryItem {
	if len(entry.Batch) > 0 {
		return entry.Batch
	}
	if entry.RequestID == "" {
		return nil
	}
	return []EntryItem{{
		RequestID: entry.RequestID,
		Response:  entry.Response,
	}}
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func cloneEntry(entry Entry) Entry {
	batch := make([]EntryItem, 0, len(entry.Batch))
	for _, item := range entry.Batch {
		batch = append(batch, cloneEntryItem(item))
	}
	return Entry{
		RequestID: entry.RequestID,
		Response:  cloneMap(entry.Response),
		Batch:     batch,
	}
}

func cloneEntryItem(item EntryItem) EntryItem {
	return EntryItem{
		RequestID: item.RequestID,
		Response:  cloneMap(item.Response),
	}
}

func canonicalRequestID(id any) (string, bool) {
	if id == nil {
		return "", false
	}
	return fmt.Sprintf("%v", id), true
}
