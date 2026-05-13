package unreplicated

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"

	netx "aegean/net"
	"aegean/telemetry"
)

type WorkflowFunc func(e *Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any
type InitStateFunc func(e *Engine) map[string]string

type Engine struct {
	Name      string
	RunConfig map[string]any

	stateMu sync.RWMutex
	kvStore map[string]string

	contextMu sync.Mutex
	contexts  map[string]map[string]any

	nestedMu       sync.Mutex
	nestedResponse map[string][]map[string]any
	nestedReadyCh  map[string]chan struct{}
}

func (e *Engine) GetRunConfig() map[string]any {
	return e.RunConfig
}

func NewEngine(name string, runConfig map[string]any, initStateFn InitStateFunc) *Engine {
	e := &Engine{
		Name:           name,
		RunConfig:      runConfig,
		kvStore:        map[string]string{},
		contexts:       map[string]map[string]any{},
		nestedResponse: map[string][]map[string]any{},
		nestedReadyCh:  map[string]chan struct{}{},
	}
	if initStateFn != nil {
		if initial := initStateFn(e); initial != nil {
			e.kvStore = copyStringMap(initial)
		}
	}
	return e
}

func (e *Engine) ReadKV(key string) string {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	return e.kvStore[key]
}

func (e *Engine) WriteKV(key, value string) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	e.kvStore[key] = value
}

func (e *Engine) SetRequestContextValue(requestID any, key string, value any) bool {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return false
	}
	e.contextMu.Lock()
	defer e.contextMu.Unlock()
	if _, exists := e.contexts[canonicalID]; !exists {
		e.contexts[canonicalID] = map[string]any{}
	}
	e.contexts[canonicalID][key] = value
	return true
}

func (e *Engine) GetRequestContextValue(requestID any, key string) (any, bool) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return nil, false
	}
	e.contextMu.Lock()
	defer e.contextMu.Unlock()
	requestCtx, exists := e.contexts[canonicalID]
	if !exists {
		return nil, false
	}
	value, exists := requestCtx[key]
	return value, exists
}

func (e *Engine) DeleteRequestContextValue(requestID any, key string) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return
	}
	e.contextMu.Lock()
	defer e.contextMu.Unlock()
	requestCtx, exists := e.contexts[canonicalID]
	if !exists {
		return
	}
	delete(requestCtx, key)
	if len(requestCtx) == 0 {
		delete(e.contexts, canonicalID)
	}
}

func (e *Engine) ClearRequestContext(requestID any) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return
	}
	e.contextMu.Lock()
	defer e.contextMu.Unlock()
	delete(e.contexts, canonicalID)
}

func (e *Engine) DispatchNestedRequestDirect(sourceRequest map[string]any, targets []string, outgoing map[string]any) {
	if outgoing == nil || len(targets) == 0 {
		return
	}
	prepared := cloneMapAny(outgoing)
	prepared["sender"] = e.Name
	telemetry.InjectContext(telemetry.ExtractContext(context.Background(), sourceRequest), prepared)

	serviceTargets := append([]string{}, targets...)
	sort.Strings(serviceTargets)
	for _, target := range serviceTargets {
		duplicated := cloneMapAny(prepared)
		go func(target string, outgoing map[string]any) {
			_, _ = netx.SendMessage(target, 8000, outgoing)
		}(target, duplicated)
	}
}

func (e *Engine) DispatchNestedRequestEO(sourceRequest map[string]any, targets []string, outgoing map[string]any) {
	e.DispatchNestedRequestDirect(sourceRequest, targets, outgoing)
}

func (e *Engine) BufferNestedResponse(payload map[string]any) bool {
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

	e.nestedMu.Lock()
	defer e.nestedMu.Unlock()
	e.nestedResponse[requestID] = append(e.nestedResponse[requestID], cloneMapAny(payload))
	readyCh := e.ensureNestedReadyChLocked(requestID)
	select {
	case readyCh <- struct{}{}:
	default:
	}
	return true
}

func (e *Engine) GetNestedResponses(requestID any) ([]map[string]any, bool) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return nil, false
	}
	e.nestedMu.Lock()
	defer e.nestedMu.Unlock()
	queue := e.nestedResponse[canonicalID]
	if len(queue) == 0 {
		return nil, false
	}
	out := make([]map[string]any, 0, len(queue))
	for _, item := range queue {
		out = append(out, cloneMapAny(item))
	}
	return out, true
}

func (e *Engine) ClearNestedResponses(requestID any) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return
	}
	e.nestedMu.Lock()
	defer e.nestedMu.Unlock()
	delete(e.nestedResponse, canonicalID)
	delete(e.nestedReadyCh, canonicalID)
}

func (e *Engine) WaitForNestedResponse(requestID any) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return
	}
	e.nestedMu.Lock()
	readyCh := e.ensureNestedReadyChLocked(canonicalID)
	e.nestedMu.Unlock()
	<-readyCh
}

func (e *Engine) ensureNestedReadyChLocked(requestID string) chan struct{} {
	readyCh, exists := e.nestedReadyCh[requestID]
	if !exists {
		readyCh = make(chan struct{}, 1)
		e.nestedReadyCh[requestID] = readyCh
	}
	return readyCh
}

func canonicalRequestID(id any) (string, bool) {
	if id == nil {
		return "", false
	}
	switch v := id.(type) {
	case string:
		return v, true
	case int:
		return strconv.Itoa(v), true
	case int64:
		return strconv.FormatInt(v, 10), true
	case float64:
		if math.Trunc(v) == v {
			return strconv.FormatInt(int64(v), 10), true
		}
		return strconv.FormatFloat(v, 'f', -1, 64), true
	case float32:
		f := float64(v)
		if math.Trunc(f) == f {
			return strconv.FormatInt(int64(f), 10), true
		}
		return strconv.FormatFloat(f, 'f', -1, 32), true
	default:
		return fmt.Sprintf("%v", v), true
	}
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

func copyStringMap(src map[string]string) map[string]string {
	out := make(map[string]string, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}
