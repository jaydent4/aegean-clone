package eo

import (
	"fmt"
	"sync"

	"aegean/common"
)

type EO struct {
	name    string
	execute ExecuteFunc
	commit  CommitFunc
	forward ForwardFunc
	box     ConsensusBox

	processMu         sync.Mutex
	mu                sync.Mutex
	log               map[uint64]Entry
	learned           uint64
	processed         uint64
	requestAttempts   map[string]struct{}
	learnedSlots      map[uint64]struct{}
	committedRequests map[string]struct{}
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

	box, err := factory(BoxConfig{
		Name:            cfg.Name,
		Peers:           append([]string{}, cfg.Peers...),
		SendRaft:        cfg.SendRaft,
		TickInterval:    cfg.TickInterval,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		MaxInflightMsgs: cfg.MaxInflightMsgs,
		MaxSizePerMsg:   cfg.MaxSizePerMsg,
	}, e.Learn)
	if err != nil {
		return nil, err
	}
	e.box = box
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
	if e.box != nil {
		e.box.Stop()
	}
}

func (e *EO) ProposeResponsePayload(requestID string, payload map[string]any) error {
	if requestID == "" {
		return fmt.Errorf("eo propose requires a request_id")
	}
	return e.box.Propose(Entry{
		RequestID: requestID,
		Response:  cloneMap(payload),
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

// Learn is the callback that the consensus box invokes when a slot is learned.
func (e *EO) Learn(slot uint64, entry Entry) {
	e.mu.Lock()
	if _, exists := e.learnedSlots[slot]; exists {
		e.mu.Unlock()
		return
	}
	e.learnedSlots[slot] = struct{}{}
	e.log[slot] = entry
	if slot > e.learned {
		e.learned = slot
	}
	e.mu.Unlock()

	e.Process()
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
		if _, duplicate := e.committedRequests[entry.RequestID]; duplicate {
			continue
		}
		e.committedRequests[entry.RequestID] = struct{}{}
		committedEntries = append(committedEntries, CommittedEntry{
			Slot:  nextSlot,
			Entry: entry,
		})
	}
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

func canonicalRequestID(id any) (string, bool) {
	if id == nil {
		return "", false
	}
	return fmt.Sprintf("%v", id), true
}
