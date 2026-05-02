package pbeo

import (
	"fmt"
	"sync"

	"aegean/components/eo"
)

type candidateSubmission struct {
	requestID       string
	response        map[string]any
	writes          map[string]string
	readVersions    map[string]uint64
	snapshotVersion uint64
	result          chan candidateResult
}

type candidateResult struct {
	retry bool
	err   error
}

type PBEO struct {
	name      string
	peers     []string
	clients   []string
	execute   ExecuteRequestFunc
	send      SendFunc
	log       *eo.EO
	runConfig map[string]any

	mu                sync.Mutex
	requestAttempts   map[string]struct{}
	committedRequests map[string]Entry

	stateMu      sync.RWMutex
	kv           map[string]string
	keyVersions  map[string]uint64
	stateVersion uint64

	commitCh        chan candidateSubmission
	nestedResponses *nestedResponseStore
	contextStore    *requestContextStore
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
			return nil, fmt.Errorf("pbeo requires a raft sender when using the default EO consensus box")
		}
	}

	initial := map[string]string{}
	if cfg.InitState != nil {
		if custom := cfg.InitState(cfg.RunConfig); custom != nil {
			initial = copyStringMap(custom)
		}
	}
	keyVersions := make(map[string]uint64, len(initial))
	for key := range initial {
		keyVersions[key] = 0
	}

	p := &PBEO{
		name:              cfg.Name,
		peers:             append([]string{}, cfg.Peers...),
		clients:           append([]string{}, cfg.Clients...),
		execute:           cfg.Execute,
		send:              cfg.Send,
		runConfig:         cloneRunConfig(cfg.RunConfig),
		requestAttempts:   make(map[string]struct{}),
		committedRequests: make(map[string]Entry),
		kv:                initial,
		keyVersions:       keyVersions,
		commitCh:          make(chan candidateSubmission, 1024),
		nestedResponses:   newNestedResponseStore(),
		contextStore:      newRequestContextStore(),
	}

	log, err := eo.NewEO(eo.Config{
		Name:            cfg.Name,
		Peers:           append([]string{}, cfg.Peers...),
		Commit:          p.handleCommittedEOEntry,
		SendRaft:        cfg.SendRaft,
		BoxFactory:      factory,
		TickInterval:    cfg.TickInterval,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		MaxInflightMsgs: cfg.MaxInflightMsgs,
		MaxSizePerMsg:   cfg.MaxSizePerMsg,
	})
	if err != nil {
		return nil, err
	}
	p.log = log

	go p.runCommitSequencer()
	return p, nil
}

func (p *PBEO) Name() string {
	return p.name
}

func (p *PBEO) IsLeader() bool {
	return p.log.IsLeader()
}

func (p *PBEO) Leader() (string, bool) {
	return p.log.Leader()
}

func (p *PBEO) Ready() bool {
	_, ok := p.log.Leader()
	return ok
}

func (p *PBEO) Stop() {
	if p.log != nil {
		p.log.Stop()
	}
}

func (p *PBEO) HandleMessage(payload map[string]any) map[string]any {
	switch payloadType, _ := payload["type"].(string); payloadType {
	case MessageTypeRaft:
		return p.log.HandleRaftMessage(payload)
	case "response":
		if p.BufferNestedResponse(payload) {
			return map[string]any{"status": "buffered_nested_response", "request_id": payload["request_id"]}
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

	if !p.log.IsLeader() {
		leader, ok := p.log.Leader()
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

	go p.executeUntilProposed(requestID, requestCopy)
	return map[string]any{"status": "accepted", "request_id": requestID, "sender": p.name}
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

func (p *PBEO) LearnedIndex() uint64 {
	return p.log.LearnedIndex()
}

func (p *PBEO) ProcessedIndex() uint64 {
	return p.log.ProcessedIndex()
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

	for {
		if _, ok := p.committedEntry(requestID); ok {
			return
		}

		snapshot := p.snapshot()
		tx := newTxn(p, requestID, snapshot)
		response := p.execute(tx, cloneMapAny(request))
		if response == nil {
			response = map[string]any{}
		}
		if _, ok := response["request_id"]; !ok {
			response["request_id"] = requestID
		}

		resultCh := make(chan candidateResult, 1)
		submission := candidateSubmission{
			requestID:       requestID,
			response:        cloneMapAny(response),
			writes:          tx.Writes(),
			readVersions:    tx.ReadVersions(),
			snapshotVersion: tx.SnapshotVersion(),
			result:          resultCh,
		}

		p.commitCh <- submission
		result := <-resultCh
		if result.retry {
			continue
		}
		if result.err != nil {
			p.finishRequestFailure(requestID)
		}
		return
	}
}

func (p *PBEO) runCommitSequencer() {
	for submission := range p.commitCh {
		if _, ok := p.committedEntry(submission.requestID); ok {
			submission.result <- candidateResult{}
			continue
		}
		if !p.validateCandidate(submission) {
			submission.result <- candidateResult{retry: true}
			continue
		}

		entry := Entry{
			RequestID:       submission.requestID,
			Response:        cloneMapAny(submission.response),
			Writes:          copyStringMap(submission.writes),
			ReadVersions:    copyUint64Map(submission.readVersions),
			SnapshotVersion: submission.snapshotVersion,
		}
		if err := p.log.ProposeResponsePayload(entry.RequestID, encodeEntryPayload(entry)); err != nil {
			submission.result <- candidateResult{err: err}
			continue
		}
		submission.result <- candidateResult{}
	}
}

func (p *PBEO) validateCandidate(submission candidateSubmission) bool {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	for key, observedVersion := range submission.readVersions {
		if p.keyVersions[key] != observedVersion {
			return false
		}
	}
	return true
}

func (p *PBEO) snapshot() stateSnapshot {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return stateSnapshot{
		kv:      copyStringMap(p.kv),
		version: copyUint64Map(p.keyVersions),
		index:   p.stateVersion,
	}
}

func (p *PBEO) handleCommittedEOEntry(committed eo.CommittedEntry) {
	entry := decodeEntryPayload(committed.Entry.RequestID, committed.Entry.Response)
	p.mu.Lock()
	if _, duplicate := p.committedRequests[entry.RequestID]; duplicate {
		p.mu.Unlock()
		return
	}
	p.committedRequests[entry.RequestID] = cloneEntry(entry)
	delete(p.requestAttempts, entry.RequestID)
	p.mu.Unlock()

	p.applyCommittedEntry(CommittedEntry{
		Slot:  committed.Slot,
		Entry: entry,
	})
}

func (p *PBEO) applyCommittedEntry(committed CommittedEntry) {
	entry := committed.Entry
	p.stateMu.Lock()
	for key, value := range entry.Writes {
		p.kv[key] = value
		p.keyVersions[key] = committed.Slot
	}
	p.stateVersion = committed.Slot
	p.stateMu.Unlock()

	p.contextStore.clear(entry.RequestID)
	p.nestedResponses.clear(entry.RequestID)
	p.sendCommittedResponse(entry)
}

func (p *PBEO) sendCommittedResponse(entry Entry) {
	if len(p.clients) == 0 {
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
}

func (p *PBEO) committedEntry(requestID string) (Entry, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.committedRequests[requestID]
	return cloneEntry(entry), ok
}

func cloneEntry(entry Entry) Entry {
	return Entry{
		RequestID:       entry.RequestID,
		Response:        cloneMapAny(entry.Response),
		Writes:          copyStringMap(entry.Writes),
		ReadVersions:    copyUint64Map(entry.ReadVersions),
		SnapshotVersion: entry.SnapshotVersion,
	}
}

func cloneRunConfig(src map[string]any) map[string]any {
	if src == nil {
		return map[string]any{}
	}
	return cloneMapAny(src)
}

func encodeEntryPayload(entry Entry) map[string]any {
	return map[string]any{
		"request_id":       entry.RequestID,
		"response":         cloneMapAny(entry.Response),
		"writes":           stringMapToAnyMap(entry.Writes),
		"read_versions":    uint64MapToAnyMap(entry.ReadVersions),
		"snapshot_version": entry.SnapshotVersion,
	}
}

func decodeEntryPayload(requestID string, payload map[string]any) Entry {
	entry := Entry{
		RequestID:       requestID,
		Response:        map[string]any{},
		Writes:          map[string]string{},
		ReadVersions:    map[string]uint64{},
		SnapshotVersion: decodeUint64(payload["snapshot_version"]),
	}
	if encodedRequestID, ok := canonicalRequestID(payload["request_id"]); ok {
		entry.RequestID = encodedRequestID
	}
	if response, ok := payload["response"].(map[string]any); ok {
		entry.Response = cloneMapAny(response)
	}
	entry.Writes = decodeStringMap(payload["writes"])
	entry.ReadVersions = decodeUint64Map(payload["read_versions"])
	return entry
}

func stringMapToAnyMap(src map[string]string) map[string]any {
	out := make(map[string]any, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}

func uint64MapToAnyMap(src map[string]uint64) map[string]any {
	out := make(map[string]any, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}

func decodeStringMap(raw any) map[string]string {
	out := map[string]string{}
	switch typed := raw.(type) {
	case map[string]string:
		return copyStringMap(typed)
	case map[string]any:
		for key, value := range typed {
			out[key] = fmt.Sprintf("%v", value)
		}
	}
	return out
}

func decodeUint64Map(raw any) map[string]uint64 {
	out := map[string]uint64{}
	switch typed := raw.(type) {
	case map[string]uint64:
		return copyUint64Map(typed)
	case map[string]int:
		for key, value := range typed {
			if value >= 0 {
				out[key] = uint64(value)
			}
		}
	case map[string]any:
		for key, value := range typed {
			out[key] = decodeUint64(value)
		}
	}
	return out
}

func decodeUint64(raw any) uint64 {
	switch typed := raw.(type) {
	case uint64:
		return typed
	case uint:
		return uint64(typed)
	case uint32:
		return uint64(typed)
	case int:
		if typed >= 0 {
			return uint64(typed)
		}
	case int64:
		if typed >= 0 {
			return uint64(typed)
		}
	case float64:
		if typed >= 0 {
			return uint64(typed)
		}
	case float32:
		if typed >= 0 {
			return uint64(typed)
		}
	}
	return 0
}
