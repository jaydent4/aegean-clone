package pbeo

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"aegean/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type stateSnapshot struct {
	kv map[string]string
}

// Txn is the sandbox a PBEO leader gives to application code.
// Reads come from an immutable committed-state snapshot plus local writes.
// Writes are recorded as a delta and become visible globally only after Raft
// commits the corresponding Entry.
type Txn struct {
	pbeo            *PBEO
	requestID       string
	snapshot        stateSnapshot
	writes          map[string]string
	requestContexts *requestContextStore
}

func newTxn(component *PBEO, requestID string, snapshot stateSnapshot) *Txn {
	return &Txn{
		pbeo:            component,
		requestID:       requestID,
		snapshot:        snapshot,
		writes:          make(map[string]string),
		requestContexts: component.contextStore,
	}
}

func (t *Txn) GetRunConfig() map[string]any {
	return t.pbeo.runConfig
}

func (t *Txn) ReadKV(key string) string {
	if value, ok := t.writes[key]; ok {
		return value
	}
	return t.snapshot.kv[key]
}

func (t *Txn) WriteKV(key, value string) {
	t.writes[key] = value
}

func (t *Txn) Writes() map[string]string {
	return copyStringMap(t.writes)
}

func (t *Txn) DispatchNestedRequestDirect(sourceRequest map[string]any, targets []string, outgoing map[string]any) {
	if outgoing == nil || len(targets) == 0 {
		return
	}

	prepared := cloneMapAny(outgoing)
	prepared["sender"] = t.pbeo.name
	telemetry.InjectContext(telemetry.ExtractContext(context.Background(), sourceRequest), prepared)

	serviceTargets := append([]string{}, targets...)
	sort.Strings(serviceTargets)
	for _, target := range serviceTargets {
		duplicated := cloneMapAny(prepared)
		go func(target string, payload map[string]any) {
			_ = t.pbeo.sendMessage(target, payload)
		}(target, duplicated)
	}
}

func (t *Txn) DispatchNestedRequestEO(sourceRequest map[string]any, targets []string, outgoing map[string]any) {
	t.DispatchNestedRequestDirect(sourceRequest, targets, outgoing)
}

func (t *Txn) GetNestedResponses(requestID any) ([]map[string]any, bool) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return nil, false
	}
	return t.pbeo.nestedResponses.get(canonicalID)
}

func (t *Txn) WaitForNestedResponse(requestID any) ([]map[string]any, bool) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return nil, false
	}
	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		"pbeo.nested_response.wait",
		trace.WithAttributes(
			attribute.String("node.name", t.pbeo.name),
			attribute.String("request.id", canonicalID),
			attribute.Int64("pbeo.executing_requests", t.pbeo.executingRequests.Load()),
		),
	)
	responses := t.pbeo.nestedResponses.wait(canonicalID)
	span.SetAttributes(attribute.Int("pbeo.nested_response.count", len(responses)))
	span.End()
	return responses, true
}

func (t *Txn) SetRequestContextValue(requestID any, key string, value any) bool {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok || key == "" {
		return false
	}
	t.requestContexts.set(canonicalID, key, value)
	return true
}

func (t *Txn) GetRequestContextValue(requestID any, key string) (any, bool) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok || key == "" {
		return nil, false
	}
	return t.requestContexts.get(canonicalID, key)
}

func (t *Txn) DeleteRequestContextValue(requestID any, key string) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok || key == "" {
		return
	}
	t.requestContexts.delete(canonicalID, key)
}

func (t *Txn) ClearRequestContext(requestID any) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return
	}
	t.requestContexts.clear(canonicalID)
}

type nestedResponseStore struct {
	mu        sync.Mutex
	cond      *sync.Cond
	responses map[string][]map[string]any
	pending   map[string][]map[string]any
}

func newNestedResponseStore() *nestedResponseStore {
	store := &nestedResponseStore{
		responses: make(map[string][]map[string]any),
		pending:   make(map[string][]map[string]any),
	}
	store.cond = sync.NewCond(&store.mu)
	return store
}

func (s *nestedResponseStore) enqueue(requestID string, payload map[string]any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending[requestID] = append(s.pending[requestID], cloneMapAny(payload))
	s.cond.Broadcast()
	return true
}

func (s *nestedResponseStore) get(requestID string) ([]map[string]any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue := s.responses[requestID]
	if len(queue) == 0 {
		return nil, false
	}
	return cloneMapSlice(queue), true
}

func (s *nestedResponseStore) promoteOne(requestID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.promoteOneLocked(requestID)
}

func (s *nestedResponseStore) promoteOneLocked(requestID string) bool {
	pending := s.pending[requestID]
	if len(pending) == 0 {
		return false
	}
	response := pending[0]
	if len(pending) == 1 {
		delete(s.pending, requestID)
	} else {
		s.pending[requestID] = pending[1:]
	}
	s.responses[requestID] = append(s.responses[requestID], response)
	return true
}

func (s *nestedResponseStore) wait(requestID string) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.responses[requestID]) == 0 {
		if s.promoteOneLocked(requestID) {
			break
		}
		s.cond.Wait()
	}
	return cloneMapSlice(s.responses[requestID])
}

func (s *nestedResponseStore) clear(requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.responses, requestID)
	delete(s.pending, requestID)
}

type requestContextStore struct {
	mu     sync.Mutex
	values map[string]map[string]any
}

func newRequestContextStore() *requestContextStore {
	return &requestContextStore{values: make(map[string]map[string]any)}
}

func (s *requestContextStore) set(requestID string, key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.values[requestID]; !ok {
		s.values[requestID] = make(map[string]any)
	}
	s.values[requestID][key] = value
}

func (s *requestContextStore) get(requestID string, key string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	values, ok := s.values[requestID]
	if !ok {
		return nil, false
	}
	value, ok := values[key]
	return value, ok
}

func (s *requestContextStore) delete(requestID string, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if values, ok := s.values[requestID]; ok {
		delete(values, key)
		if len(values) == 0 {
			delete(s.values, requestID)
		}
	}
}

func (s *requestContextStore) clear(requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.values, requestID)
}

func canonicalRequestID(id any) (string, bool) {
	if id == nil {
		return "", false
	}
	return fmt.Sprintf("%v", id), true
}

func cloneMapAny(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	out := make(map[string]any, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}

func cloneMapSlice(src []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(src))
	for _, item := range src {
		out = append(out, cloneMapAny(item))
	}
	return out
}

func copyStringMap(src map[string]string) map[string]string {
	out := make(map[string]string, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}
