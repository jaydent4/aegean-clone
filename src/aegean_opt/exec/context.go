package exec

import "sync"

type requestContextStore struct {
	mu       sync.Mutex
	contexts map[string]map[string]any
}

func newRequestContextStore() *requestContextStore {
	return &requestContextStore{
		contexts: make(map[string]map[string]any),
	}
}

func (e *Exec) SetRequestContextValue(requestID any, key string, value any) bool {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return false
	}
	e.scheduler.contextStore.set(canonicalID, key, value)
	return true
}

func (e *Exec) GetRequestContextValue(requestID any, key string) (any, bool) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return nil, false
	}
	return e.scheduler.contextStore.get(canonicalID, key)
}

func (e *Exec) ClearRequestContext(requestID any) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return
	}
	e.scheduler.contextStore.clearByID(canonicalID)
}

func (e *Exec) DeleteRequestContextValue(requestID any, key string) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return
	}
	e.scheduler.contextStore.delete(canonicalID, key)
}

func (s *requestContextStore) set(requestID string, key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.contexts[requestID]; !exists {
		s.contexts[requestID] = make(map[string]any)
	}
	s.contexts[requestID][key] = value
}

func (s *requestContextStore) get(requestID string, key string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, exists := s.contexts[requestID]
	if !exists {
		return nil, false
	}
	value, exists := ctx[key]
	if !exists {
		return nil, false
	}
	return value, true
}

func (s *requestContextStore) clearByID(requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.contexts, requestID)
}

func (s *requestContextStore) clearByIDExcept(requestID string, keepKeys ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, exists := s.contexts[requestID]
	if !exists {
		return
	}
	if len(keepKeys) == 0 {
		delete(s.contexts, requestID)
		return
	}
	keep := make(map[string]struct{}, len(keepKeys))
	for _, key := range keepKeys {
		keep[key] = struct{}{}
	}
	filtered := make(map[string]any)
	for key, value := range ctx {
		if _, ok := keep[key]; ok {
			filtered[key] = value
		}
	}
	if len(filtered) == 0 {
		delete(s.contexts, requestID)
		return
	}
	s.contexts[requestID] = filtered
}

func (s *requestContextStore) delete(requestID string, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, exists := s.contexts[requestID]
	if !exists {
		return
	}
	delete(ctx, key)
	if len(ctx) == 0 {
		delete(s.contexts, requestID)
	}
}
