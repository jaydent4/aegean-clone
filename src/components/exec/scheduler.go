package exec

import (
	"sync"
)

type execScheduler struct {
	mu               sync.Mutex
	inflightRequests map[string]*scheduledRequest
	nestedResponses  map[string][]map[string]any
	nestedReadyCh    chan struct{}
	contextStore     *requestContextStore
	parallelWindowK  int
}

func newExecScheduler() *execScheduler {
	return &execScheduler{
		inflightRequests: make(map[string]*scheduledRequest),
		nestedResponses:  make(map[string][]map[string]any),
		nestedReadyCh:    make(chan struct{}, 1),
		contextStore:     newRequestContextStore(),
		parallelWindowK:  4,
	}
}

func (s *execScheduler) enqueueNestedResponse(requestID string, payload map[string]any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.inflightRequests[requestID]; !exists {
		return false
	}
	s.nestedResponses[requestID] = append(s.nestedResponses[requestID], payload)
	select {
	case s.nestedReadyCh <- struct{}{}:
	default:
	}
	return true
}

func (s *execScheduler) hasNestedResponse(requestID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.nestedResponses[requestID]) > 0
}

func (s *execScheduler) popNestedResponse(requestID string) (map[string]any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue := s.nestedResponses[requestID]
	if len(queue) == 0 {
		return nil, false
	}
	nested := queue[0]
	if len(queue) == 1 {
		delete(s.nestedResponses, requestID)
	} else {
		s.nestedResponses[requestID] = queue[1:]
	}
	return nested, true
}

func (e *Exec) executeParallelBatches(parallelBatches [][]map[string]any, ndSeed int64, ndTimestamp float64) []map[string]any {
	return e.scheduler.executeParallelBatches(e, parallelBatches, ndSeed, ndTimestamp)
}

func (e *Exec) executeSequentialBatches(parallelBatches [][]map[string]any, ndSeed int64, ndTimestamp float64) []map[string]any {
	return e.scheduler.executeSequentialBatches(e, parallelBatches, ndSeed, ndTimestamp)
}

func (s *execScheduler) registerScheduledRequests(requests []*scheduledRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, req := range requests {
		s.inflightRequests[req.id] = req
	}
}

func (s *execScheduler) unregisterScheduledRequests(e *Exec, requests []*scheduledRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, req := range requests {
		delete(s.inflightRequests, req.id)
		delete(s.nestedResponses, req.id)
		s.contextStore.clearByID(req.id)
	}
}
