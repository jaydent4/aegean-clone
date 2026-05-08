package exec

import "sync"

type NestedEOReplicator interface {
	IsLeader() bool
	Leader() (string, bool)
	ProposeResponsePayload(requestID string, payload map[string]any) error
}

type eoDispatchAction uint8

type nestedDispatchState uint8

const (
	eoDispatchSkip eoDispatchAction = iota
	eoDispatchSendDirect
	eoDispatchWaitForResponse
)

const (
	nestedDispatchPending nestedDispatchState = iota
	nestedDispatchProposed
	nestedDispatchCompleted
)

const (
	nestedResponseIgnoredStatus          = "eo_nested_response_ignored"
	nestedResponseWaitingForLeaderStatus = "eo_nested_response_waiting_for_leader"
	nestedResponseProposedStatus         = "eo_nested_response_proposed"
	nestedResponseAlreadyProposedStatus  = "eo_nested_response_already_proposed"
)

type nestedDispatchTracker struct {
	mu     sync.Mutex
	states map[string]nestedDispatchState
}

func newNestedDispatchTracker() *nestedDispatchTracker {
	return &nestedDispatchTracker{
		states: make(map[string]nestedDispatchState),
	}
}

func (e *Exec) SetNestedEO(replication NestedEOReplicator) {
	e.nestedEO = replication
}

func (e *Exec) NestedEOReady() bool {
	if e.nestedEO == nil {
		return true
	}
	_, ok := e.nestedEO.Leader()
	return ok
}

func (e *Exec) DispatchNestedRequestEO(sourceRequest map[string]any, targets []string, outgoing map[string]any) {
	prepared, ok := e.prepareNestedDispatchPayload(sourceRequest, outgoing)
	if !ok || len(targets) == 0 || e.nestedEO == nil {
		return
	}

	switch e.nextEODispatchAction(prepared) {
	case eoDispatchSendDirect:
		sendNestedRequestDirect(targets, prepared)
	case eoDispatchWaitForResponse:
		return
	case eoDispatchSkip:
		return
	}
}

func (e *Exec) ClaimNestedRequestEO(prepared map[string]any) bool {
	if prepared == nil || e.nestedEO == nil {
		return false
	}
	return e.nextEODispatchAction(prepared) == eoDispatchSendDirect
}

func (e *Exec) HandleNestedResponseMessage(payload map[string]any) (map[string]any, bool) {
	requestID, state, ok := e.nestedResponseState(payload)
	if !ok {
		return nil, false
	}

	switch state {
	case nestedDispatchCompleted:
		return nestedResponseStatus(requestID, nestedResponseIgnoredStatus), true
	case nestedDispatchPending:
		return e.handlePendingNestedResponse(requestID, payload)
	case nestedDispatchProposed:
		return nestedResponseStatus(requestID, nestedResponseAlreadyProposedStatus), true
	default:
		return nil, false
	}
}

func (e *Exec) BufferExactOnceNestedResponse(payload map[string]any) bool {
	if payload == nil {
		return false
	}

	requestID, ok := canonicalRequestID(payload["request_id"])
	if ok {
		e.nestedDispatchTracker.markCompleted(requestID)
	}

	buffered := e.BufferNestedResponse(payload)
	if ok {
		e.nestedDispatchTracker.markCompleted(requestID)
	}
	return buffered
}

func (e *Exec) nextEODispatchAction(prepared map[string]any) eoDispatchAction {
	requestID, ok := canonicalRequestID(prepared["request_id"])
	if !ok {
		return eoDispatchSendDirect
	}

	if !e.nestedDispatchTracker.registerPending(requestID) {
		return eoDispatchSkip
	}

	if e.nestedEO.IsLeader() {
		return eoDispatchSendDirect
	}

	if _, ok := e.nestedEO.Leader(); !ok {
		return eoDispatchSendDirect
	}

	return eoDispatchWaitForResponse
}

func (e *Exec) nestedResponseState(payload map[string]any) (string, nestedDispatchState, bool) {
	if e.nestedEO == nil || payload == nil {
		return "", 0, false
	}

	requestID, ok := canonicalRequestID(payload["request_id"])
	if !ok {
		return "", 0, false
	}

	state, tracked := e.nestedDispatchTracker.state(requestID)
	if !tracked {
		return "", 0, false
	}

	return requestID, state, true
}

func (e *Exec) handlePendingNestedResponse(requestID string, payload map[string]any) (map[string]any, bool) {
	if !e.nestedEO.IsLeader() {
		if _, ok := e.nestedEO.Leader(); ok {
			return nestedResponseStatus(requestID, nestedResponseWaitingForLeaderStatus), true
		}
		return nil, false
	}

	if !e.nestedDispatchTracker.markProposed(requestID) {
		return nestedResponseStatus(requestID, nestedResponseIgnoredStatus), true
	}

	proposed := cloneMapAny(payload)
	proposed["shim_quorum_aggregated"] = true
	if err := e.nestedEO.ProposeResponsePayload(requestID, proposed); err != nil {
		e.nestedDispatchTracker.resetPending(requestID)
		return nil, false
	}

	return nestedResponseStatus(requestID, nestedResponseProposedStatus), true
}

func nestedResponseStatus(requestID string, status string) map[string]any {
	return map[string]any{
		"status":     status,
		"request_id": requestID,
	}
}

func (t *nestedDispatchTracker) registerPending(requestID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.states[requestID]; exists {
		return false
	}
	t.states[requestID] = nestedDispatchPending
	return true
}

func (t *nestedDispatchTracker) state(requestID string) (nestedDispatchState, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.states[requestID]
	return state, ok
}

func (t *nestedDispatchTracker) markProposed(requestID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if state, ok := t.states[requestID]; !ok || state != nestedDispatchPending {
		return false
	}
	t.states[requestID] = nestedDispatchProposed
	return true
}

func (t *nestedDispatchTracker) resetPending(requestID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if state, ok := t.states[requestID]; ok && state == nestedDispatchProposed {
		t.states[requestID] = nestedDispatchPending
	}
}

func (t *nestedDispatchTracker) markCompleted(requestID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.states[requestID] = nestedDispatchCompleted
}
