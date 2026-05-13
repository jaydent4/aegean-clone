package exec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	netx "aegean/net"
)

const (
	MessageTypeEONestedRequest = "eo_nested_request"

	// TODO: make the nested EO request quorum configurable once configs settle.
	nestedEORequestQuorumSize = 2
	// TODO: make the fallback timeout configurable and revisit recovery policy.
	nestedEORequestQuorumTimeout = 5 * time.Second
	nestedEORequestForwardRetry  = 25 * time.Millisecond
	nestedEORequestForwardRPC    = 200 * time.Millisecond
)

type NestedEORequestQuorumGate struct {
	name    string
	inner   NestedEOReplicator
	timeout time.Duration

	mu        sync.Mutex
	windows   map[string]*nestedEORequestWindow
	completed map[string]struct{}
}

type nestedEORequestWindow struct {
	requestID  string
	buckets    map[string]*nestedEORequestBucket
	timer      *time.Timer
	dispatched bool
}

type nestedEORequestBucket struct {
	hash    string
	request map[string]any
	targets []string
	senders map[string]struct{}
}

type nestedEOSelectedRequest struct {
	requestID string
	hash      string
	request   map[string]any
	targets   []string
	fallback  bool
	variants  string
}

func NewNestedEORequestQuorumGate(name string, inner NestedEOReplicator) *NestedEORequestQuorumGate {
	return newNestedEORequestQuorumGate(name, inner, nestedEORequestQuorumTimeout)
}

func newNestedEORequestQuorumGate(name string, inner NestedEOReplicator, timeout time.Duration) *NestedEORequestQuorumGate {
	if timeout <= 0 {
		timeout = nestedEORequestQuorumTimeout
	}
	return &NestedEORequestQuorumGate{
		name:      name,
		inner:     inner,
		timeout:   timeout,
		windows:   make(map[string]*nestedEORequestWindow),
		completed: make(map[string]struct{}),
	}
}

func (q *NestedEORequestQuorumGate) IsLeader() bool {
	return q.inner != nil && q.inner.IsLeader()
}

func (q *NestedEORequestQuorumGate) Leader() (string, bool) {
	if q.inner == nil {
		return "", false
	}
	return q.inner.Leader()
}

func (q *NestedEORequestQuorumGate) ProposeResponsePayload(requestID string, payload map[string]any) error {
	if q.inner == nil {
		return fmt.Errorf("nested EO is not configured")
	}
	return q.inner.ProposeResponsePayload(requestID, payload)
}

func (q *NestedEORequestQuorumGate) SubmitNestedRequest(source string, requestID string, targets []string, payload map[string]any) map[string]any {
	if q == nil || q.inner == nil {
		return map[string]any{"status": "eo_nested_request_quorum_not_configured", "request_id": requestID}
	}
	if !q.inner.IsLeader() {
		leader, ok := q.inner.Leader()
		if !ok || leader == "" {
			go q.forwardNestedRequestUntilAccepted(source, requestID, targets, payload)
			return map[string]any{"status": "eo_nested_request_waiting_for_leader", "request_id": requestID}
		}
		message := q.forwardMessage(source, requestID, targets, payload)
		go func() {
			if _, err := netx.SendMessage(leader, 8000, message, 1, nestedEORequestForwardRPC); err != nil {
				q.forwardNestedRequestUntilAccepted(source, requestID, targets, payload)
			}
		}()
		return map[string]any{"status": "eo_nested_request_forwarded", "request_id": requestID, "leader": leader}
	}
	return q.recordVote(source, requestID, targets, payload)
}

func (q *NestedEORequestQuorumGate) forwardNestedRequestUntilAccepted(source string, requestID string, targets []string, payload map[string]any) {
	deadline := time.Now().Add(q.timeout)
	for time.Now().Before(deadline) {
		if q.inner == nil {
			return
		}
		if q.inner.IsLeader() {
			q.recordVote(source, requestID, targets, payload)
			return
		}
		leader, ok := q.inner.Leader()
		if ok && leader != "" {
			message := q.forwardMessage(source, requestID, targets, payload)
			if _, err := netx.SendMessage(leader, 8000, message, 1, nestedEORequestForwardRPC); err == nil {
				return
			}
		}
		time.Sleep(nestedEORequestForwardRetry)
	}
}

func (q *NestedEORequestQuorumGate) HandleNestedRequestMessage(payload map[string]any) map[string]any {
	if q == nil || q.inner == nil {
		return map[string]any{"status": "eo_nested_request_quorum_not_configured"}
	}
	requestID, ok := canonicalRequestID(payload["request_id"])
	if !ok {
		return map[string]any{"status": "invalid_eo_nested_request", "error": "missing request_id"}
	}
	source, _ := payload["sender"].(string)
	if source == "" {
		source = "unknown"
	}
	request, ok := payload["request"].(map[string]any)
	if !ok || request == nil {
		return map[string]any{"status": "invalid_eo_nested_request", "request_id": requestID, "error": "missing request"}
	}
	return q.recordVote(source, requestID, stringSliceFromAny(payload["targets"]), request)
}

func (q *NestedEORequestQuorumGate) forwardMessage(source string, requestID string, targets []string, payload map[string]any) map[string]any {
	return map[string]any{
		"type":         MessageTypeEONestedRequest,
		"request_id":   requestID,
		"request":      cloneMapAny(payload),
		"targets":      append([]string{}, targets...),
		"request_hash": nestedEORequestHash(requestID, payload),
		"sender":       source,
	}
}

func (q *NestedEORequestQuorumGate) recordVote(source string, requestID string, targets []string, payload map[string]any) map[string]any {
	if source == "" {
		source = q.name
	}
	hash := nestedEORequestHash(requestID, payload)
	selected, status := q.recordVoteLocked(source, requestID, hash, targets, payload)
	if selected != nil {
		q.dispatchSelected(selected)
	}
	return status
}

func (q *NestedEORequestQuorumGate) recordVoteLocked(source string, requestID string, hash string, targets []string, payload map[string]any) (*nestedEOSelectedRequest, map[string]any) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, done := q.completed[requestID]; done {
		return nil, map[string]any{"status": "eo_nested_request_already_dispatched", "request_id": requestID}
	}

	window := q.windows[requestID]
	if window == nil {
		window = &nestedEORequestWindow{
			requestID: requestID,
			buckets:   make(map[string]*nestedEORequestBucket),
		}
		q.windows[requestID] = window
		window.timer = time.AfterFunc(q.timeout, func() {
			q.dispatchFallback(requestID)
		})
	}

	bucket := window.buckets[hash]
	if bucket == nil {
		bucket = &nestedEORequestBucket{
			hash:    hash,
			request: cloneMapAny(payload),
			targets: append([]string{}, targets...),
			senders: make(map[string]struct{}),
		}
		window.buckets[hash] = bucket
	}
	bucket.senders[source] = struct{}{}

	if len(bucket.senders) < nestedEORequestQuorumSize {
		return nil, map[string]any{
			"status":     "eo_nested_request_waiting_for_quorum",
			"request_id": requestID,
			"count":      len(bucket.senders),
			"hash":       hash,
		}
	}
	return q.selectBucketLocked(requestID, window, bucket, false), map[string]any{
		"status":     "eo_nested_request_quorum_reached",
		"request_id": requestID,
		"count":      len(bucket.senders),
		"hash":       hash,
	}
}

func (q *NestedEORequestQuorumGate) dispatchFallback(requestID string) {
	var selected *nestedEOSelectedRequest
	q.mu.Lock()
	window := q.windows[requestID]
	if window != nil && !window.dispatched {
		bucket := deterministicNestedEOFallbackBucket(window)
		if bucket != nil {
			selected = q.selectBucketLocked(requestID, window, bucket, true)
		}
	}
	q.mu.Unlock()

	if selected != nil {
		q.dispatchSelected(selected)
	}
}

func (q *NestedEORequestQuorumGate) selectBucketLocked(requestID string, window *nestedEORequestWindow, bucket *nestedEORequestBucket, fallback bool) *nestedEOSelectedRequest {
	if window.dispatched {
		return nil
	}
	window.dispatched = true
	if window.timer != nil {
		window.timer.Stop()
	}
	delete(q.windows, requestID)
	q.completed[requestID] = struct{}{}
	return &nestedEOSelectedRequest{
		requestID: requestID,
		hash:      bucket.hash,
		request:   cloneMapAny(bucket.request),
		targets:   append([]string{}, bucket.targets...),
		fallback:  fallback,
		variants:  nestedEORequestVariants(window),
	}
}

func (q *NestedEORequestQuorumGate) dispatchSelected(selected *nestedEOSelectedRequest) {
	if selected == nil {
		return
	}
	if selected.fallback {
		log.Printf("%s: warning: nested EO request quorum timeout request_id=%s selected_hash=%s variants=%s", q.name, selected.requestID, selected.hash, selected.variants)
	}
	outgoing := cloneMapAny(selected.request)
	outgoing["sender"] = q.name
	sendNestedRequestDirect(selected.targets, outgoing)
}

func deterministicNestedEOFallbackBucket(window *nestedEORequestWindow) *nestedEORequestBucket {
	if window == nil || len(window.buckets) == 0 {
		return nil
	}
	hashes := make([]string, 0, len(window.buckets))
	for hash := range window.buckets {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	return window.buckets[hashes[0]]
}

func nestedEORequestVariants(window *nestedEORequestWindow) string {
	if window == nil || len(window.buckets) == 0 {
		return "[]"
	}
	hashes := make([]string, 0, len(window.buckets))
	for hash := range window.buckets {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	variants := make([]string, 0, len(hashes))
	for _, hash := range hashes {
		bucket := window.buckets[hash]
		variants = append(variants, fmt.Sprintf("%s:%d", hash, len(bucket.senders)))
	}
	return "[" + joinStrings(variants, ",") + "]"
}

func nestedEORequestHash(requestID string, payload map[string]any) string {
	canonical := map[string]any{
		"request_id": requestID,
		"request":    nestedEOCanonicalValue(payload),
	}
	data, err := json.Marshal(canonical)
	if err != nil {
		data = []byte(fmt.Sprintf("%v", canonical))
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func nestedEOCanonicalValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			if key == "sender" || key == "_otel" {
				continue
			}
			out[key] = nestedEOCanonicalValue(item)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, nestedEOCanonicalValue(item))
		}
		return out
	case []string:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out
	case []map[string]any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, nestedEOCanonicalValue(item))
		}
		return out
	default:
		return value
	}
}

func stringSliceFromAny(raw any) []string {
	switch typed := raw.(type) {
	case []string:
		return append([]string{}, typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if value, ok := item.(string); ok {
				out = append(out, value)
			}
		}
		return out
	default:
		return nil
	}
}

func joinStrings(values []string, sep string) string {
	if len(values) == 0 {
		return ""
	}
	out := values[0]
	for _, value := range values[1:] {
		out += sep + value
	}
	return out
}
