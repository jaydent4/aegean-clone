package exec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	stdnet "net"
	"sort"
	"sync"
	"time"

	netx "aegean/net"
)

const (
	MessageTypeEONestedRequest      = "eo_nested_request"
	MessageTypeEONestedRequestBatch = "eo_nested_request_batch"

	// TODO: make the nested EO request quorum size configurable once configs settle.
	nestedEORequestQuorumSize = 2

	DefaultNestedEORequestQuorumTimeout       = 5 * time.Second
	DefaultNestedEORequestForwardRPCTimeout   = time.Second
	DefaultNestedEORequestForwardBatchSize    = 32
	DefaultNestedEORequestForwardBatchTimeout = 500 * time.Microsecond

	nestedEORequestForwardRetry        = 25 * time.Millisecond
	nestedEORequestQuorumStatsWindow   = time.Second
	nestedEORequestVoteSendUnixNanoKey = "_nested_eo_request_vote_send_unix_nano"
)

type NestedEORequestQuorumGate struct {
	name                string
	inner               NestedEOReplicator
	timeout             time.Duration
	forwardRPCTimeout   time.Duration
	forwardBatchSize    int
	forwardBatchTimeout time.Duration
	onSelected          func(string)

	mu        sync.Mutex
	windows   map[string]*nestedEORequestWindow
	completed map[string]struct{}
	stats     nestedEORequestQuorumStats

	forwardMu     sync.Mutex
	forwardQueues map[string][]map[string]any
	forwardTimers map[string]*time.Timer
}

type NestedEORequestQuorumGateConfig struct {
	ForwardBatchSize    int
	ForwardBatchTimeout time.Duration
}

type nestedEORequestWindow struct {
	requestID   string
	buckets     map[string]*nestedEORequestBucket
	timer       *time.Timer
	dispatched  bool
	firstVote   time.Time
	firstSource string
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

type nestedEORequestQuorumStats struct {
	windowStart                   time.Time
	votes                         int
	localVotes                    int
	remoteVotes                   int
	windows                       int
	firstLocal                    int
	firstRemote                   int
	waitingForQuorum              int
	dispatches                    int
	fallbacks                     int
	completedDuplicates           int
	hashCount                     int
	totalHash                     time.Duration
	maxHash                       time.Duration
	statusCheckCount              int
	totalStatusCheck              time.Duration
	maxStatusCheck                time.Duration
	lockCount                     int
	totalLockWait                 time.Duration
	maxLockWait                   time.Duration
	totalLocked                   time.Duration
	maxLocked                     time.Duration
	remoteTransitCount            int
	totalRemoteTransit            time.Duration
	maxRemoteTransit              time.Duration
	secondVoteSendCount           int
	totalFirstVoteToSecondSend    time.Duration
	maxFirstVoteToSecondSend      time.Duration
	secondVoteReceiveCount        int
	totalFirstVoteToSecondReceive time.Duration
	maxFirstVoteToSecondReceive   time.Duration
	totalFirstVoteToSelect        time.Duration
	maxFirstVoteToSelect          time.Duration
	dispatchSelectedCount         int
	totalDispatchSelected         time.Duration
	maxDispatchSelected           time.Duration
}

func NewNestedEORequestQuorumGate(name string, inner NestedEOReplicator) *NestedEORequestQuorumGate {
	return NewNestedEORequestQuorumGateWithConfig(name, inner, defaultNestedEORequestQuorumGateConfig())
}

func newNestedEORequestQuorumGate(name string, inner NestedEOReplicator, timeout time.Duration) *NestedEORequestQuorumGate {
	gate := NewNestedEORequestQuorumGate(name, inner)
	gate.timeout = timeout
	return gate
}

func NewNestedEORequestQuorumGateWithConfig(name string, inner NestedEOReplicator, config NestedEORequestQuorumGateConfig) *NestedEORequestQuorumGate {
	config = normalizeNestedEORequestQuorumGateConfig(config)
	return &NestedEORequestQuorumGate{
		name:                name,
		inner:               inner,
		timeout:             DefaultNestedEORequestQuorumTimeout,
		forwardRPCTimeout:   DefaultNestedEORequestForwardRPCTimeout,
		forwardBatchSize:    config.ForwardBatchSize,
		forwardBatchTimeout: config.ForwardBatchTimeout,
		windows:             make(map[string]*nestedEORequestWindow),
		completed:           make(map[string]struct{}),
		stats:               nestedEORequestQuorumStats{windowStart: time.Now()},

		forwardQueues: make(map[string][]map[string]any),
		forwardTimers: make(map[string]*time.Timer),
	}
}

func defaultNestedEORequestQuorumGateConfig() NestedEORequestQuorumGateConfig {
	return NestedEORequestQuorumGateConfig{
		ForwardBatchSize:    DefaultNestedEORequestForwardBatchSize,
		ForwardBatchTimeout: DefaultNestedEORequestForwardBatchTimeout,
	}
}

func normalizeNestedEORequestQuorumGateConfig(config NestedEORequestQuorumGateConfig) NestedEORequestQuorumGateConfig {
	if config.ForwardBatchSize <= 0 {
		config.ForwardBatchSize = DefaultNestedEORequestForwardBatchSize
	}
	if config.ForwardBatchTimeout < 0 {
		config.ForwardBatchTimeout = DefaultNestedEORequestForwardBatchTimeout
	}
	return config
}

func (q *NestedEORequestQuorumGate) SetSelectedRequestHook(hook func(string)) {
	q.onSelected = hook
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
		q.enqueueForwardVote(leader, q.forwardMessage(source, requestID, targets, payload))
		return map[string]any{"status": "eo_nested_request_forwarded", "request_id": requestID, "leader": leader}
	}
	return q.recordVote(source, requestID, targets, payload, 0, time.Now())
}

func (q *NestedEORequestQuorumGate) forwardNestedRequestUntilAccepted(source string, requestID string, targets []string, payload map[string]any) {
	deadline := time.Now().Add(q.timeout)
	attempts := 0
	var lastErr error
	for time.Now().Before(deadline) {
		if q.inner == nil {
			return
		}
		if q.inner.IsLeader() {
			q.recordVote(source, requestID, targets, payload, 0, time.Now())
			return
		}
		leader, ok := q.inner.Leader()
		if ok && leader != "" {
			message := q.forwardMessage(source, requestID, targets, payload)
			attempts++
			if _, err := netx.SendMessage(leader, 8000, message, 1, q.forwardRPCTimeout); err == nil {
				return
			} else {
				lastErr = err
			}
		}
		time.Sleep(nestedEORequestForwardRetry)
	}
	log.Printf(
		"%s: warning: nested EO request forward timed out before reaching leader request_id=%s source=%s child=%s timeout=%s attempts=%d last_error=%v",
		q.name,
		requestID,
		source,
		nestedResponseChild(payload),
		q.timeout,
		attempts,
		lastErr,
	)
}

func (q *NestedEORequestQuorumGate) HandleNestedRequestMessage(payload map[string]any) map[string]any {
	if q == nil || q.inner == nil {
		return map[string]any{"status": "eo_nested_request_quorum_not_configured"}
	}
	if msgType, _ := payload["type"].(string); msgType == MessageTypeEONestedRequestBatch {
		return q.handleNestedRequestBatchMessage(payload)
	}
	requestID, ok := canonicalRequestID(payload["request_id"])
	if !ok {
		return map[string]any{"status": "invalid_eo_nested_request", "error": "missing request_id"}
	}
	if status, done := q.completedRequestStatus(requestID); done {
		return status
	}
	source, _ := payload["sender"].(string)
	if source == "" {
		source = "unknown"
	}
	request, ok := payload["request"].(map[string]any)
	if !ok || request == nil {
		return map[string]any{"status": "invalid_eo_nested_request", "request_id": requestID, "error": "missing request"}
	}
	voteSendUnixNano, _ := nestedInt64(payload[nestedEORequestVoteSendUnixNanoKey])
	return q.recordVote(source, requestID, stringSliceFromAny(payload["targets"]), request, voteSendUnixNano, time.Now())
}

func (q *NestedEORequestQuorumGate) handleNestedRequestBatchMessage(payload map[string]any) map[string]any {
	votes := nestedEORequestBatchVotes(payload["votes"])
	if len(votes) == 0 {
		return map[string]any{"status": "invalid_eo_nested_request_batch", "error": "missing votes"}
	}
	result := map[string]any{
		"status": "eo_nested_request_batch_processed",
		"votes":  len(votes),
	}
	dispatched := 0
	alreadyDispatched := 0
	for _, vote := range votes {
		status := q.HandleNestedRequestMessage(vote)
		switch status["status"] {
		case "eo_nested_request_quorum_reached":
			dispatched++
		case "eo_nested_request_already_dispatched":
			alreadyDispatched++
		}
	}
	result["dispatched"] = dispatched
	result["already_dispatched"] = alreadyDispatched
	return result
}

func (q *NestedEORequestQuorumGate) forwardMessage(source string, requestID string, targets []string, payload map[string]any) map[string]any {
	return map[string]any{
		"type":                             MessageTypeEONestedRequest,
		"request_id":                       requestID,
		"request":                          cloneMapAny(payload),
		"targets":                          append([]string{}, targets...),
		"request_hash":                     nestedEORequestHash(requestID, payload),
		"sender":                           source,
		nestedEORequestVoteSendUnixNanoKey: time.Now().UnixNano(),
	}
}

func (q *NestedEORequestQuorumGate) enqueueForwardVote(leader string, vote map[string]any) {
	if leader == "" || vote == nil {
		return
	}
	var batch []map[string]any

	q.forwardMu.Lock()
	q.forwardQueues[leader] = append(q.forwardQueues[leader], vote)
	if len(q.forwardQueues[leader]) >= q.forwardBatchSize {
		batch = q.forwardQueues[leader]
		delete(q.forwardQueues, leader)
		if timer := q.forwardTimers[leader]; timer != nil {
			timer.Stop()
			delete(q.forwardTimers, leader)
		}
	} else if q.forwardTimers[leader] == nil {
		q.forwardTimers[leader] = time.AfterFunc(q.forwardBatchTimeout, func() {
			q.flushForwardVotes(leader)
		})
	}
	q.forwardMu.Unlock()

	if len(batch) > 0 {
		go q.sendForwardVoteBatch(leader, batch)
	}
}

func (q *NestedEORequestQuorumGate) flushForwardVotes(leader string) {
	q.forwardMu.Lock()
	batch := q.forwardQueues[leader]
	delete(q.forwardQueues, leader)
	delete(q.forwardTimers, leader)
	q.forwardMu.Unlock()

	if len(batch) > 0 {
		q.sendForwardVoteBatch(leader, batch)
	}
}

func (q *NestedEORequestQuorumGate) sendForwardVoteBatch(leader string, votes []map[string]any) {
	if leader == "" || len(votes) == 0 {
		return
	}
	sendUnixNano := time.Now().UnixNano()
	for _, vote := range votes {
		vote[nestedEORequestVoteSendUnixNanoKey] = sendUnixNano
	}
	payload := map[string]any{
		"type":  MessageTypeEONestedRequestBatch,
		"votes": votes,
	}
	start := time.Now()
	if _, err := netx.SendMessage(leader, 8000, payload, 1, q.forwardRPCTimeout); err == nil {
		return
	} else {
		q.logForwardVoteBatchRPCFailure(leader, len(votes), time.Since(start), err)
	}
	for _, vote := range votes {
		source, _ := vote["sender"].(string)
		requestID, _ := canonicalRequestID(vote["request_id"])
		request, _ := vote["request"].(map[string]any)
		if requestID == "" || request == nil {
			continue
		}
		q.forwardNestedRequestUntilAccepted(source, requestID, stringSliceFromAny(vote["targets"]), request)
	}
}

func (q *NestedEORequestQuorumGate) logForwardVoteBatchRPCFailure(leader string, voteCount int, elapsed time.Duration, err error) {
	if nestedEORequestForwardErrorTimedOut(err) {
		log.Printf(
			"%s: warning: nested EO request forward batch RPC timed out leader=%s votes=%d timeout=%s elapsed=%s error=%v",
			q.name,
			leader,
			voteCount,
			q.forwardRPCTimeout,
			elapsed,
			err,
		)
		return
	}
	log.Printf(
		"%s: warning: nested EO request forward batch RPC failed leader=%s votes=%d timeout=%s elapsed=%s error=%v",
		q.name,
		leader,
		voteCount,
		q.forwardRPCTimeout,
		elapsed,
		err,
	)
}

func nestedEORequestForwardErrorTimedOut(err error) bool {
	var netErr stdnet.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func (q *NestedEORequestQuorumGate) recordVote(source string, requestID string, targets []string, payload map[string]any, voteSendUnixNano int64, voteReceiveAt time.Time) map[string]any {
	if source == "" {
		source = q.name
	}
	statusCheckStart := time.Now()
	if status, done := q.completedRequestStatus(requestID); done {
		q.recordCompletedDuplicateStats(time.Since(statusCheckStart))
		return status
	}
	statusCheckDuration := time.Since(statusCheckStart)
	if voteReceiveAt.IsZero() {
		voteReceiveAt = time.Now()
	}
	hashStart := time.Now()
	hash := nestedEORequestHash(requestID, payload)
	hashDuration := time.Since(hashStart)
	selected, status := q.recordVoteLocked(source, requestID, hash, targets, payload, voteSendUnixNano, voteReceiveAt, hashDuration, statusCheckDuration)
	if selected != nil {
		dispatchStart := time.Now()
		q.dispatchSelected(selected)
		q.recordDispatchSelectedStats(time.Since(dispatchStart))
	}
	return status
}

func (q *NestedEORequestQuorumGate) recordCompletedDuplicateStats(statusCheckDuration time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stats.completedDuplicates++
	q.recordStatusCheckStatsLocked(statusCheckDuration)
	q.maybeLogStatsLocked(time.Now())
}

func (q *NestedEORequestQuorumGate) completedRequestStatus(requestID string) (map[string]any, bool) {
	q.mu.Lock()
	_, done := q.completed[requestID]
	q.mu.Unlock()
	if !done {
		return nil, false
	}
	return map[string]any{"status": "eo_nested_request_already_dispatched", "request_id": requestID}, true
}

func (q *NestedEORequestQuorumGate) recordVoteLocked(source string, requestID string, hash string, targets []string, payload map[string]any, voteSendUnixNano int64, voteReceiveAt time.Time, hashDuration time.Duration, statusCheckDuration time.Duration) (*nestedEOSelectedRequest, map[string]any) {
	lockWaitStart := time.Now()
	q.mu.Lock()
	lockedStart := time.Now()
	lockWait := lockedStart.Sub(lockWaitStart)
	defer func() {
		lockedDuration := time.Since(lockedStart)
		q.recordLockStatsLocked(lockWait, lockedDuration)
		q.maybeLogStatsLocked(time.Now())
		q.mu.Unlock()
	}()

	q.recordVoteStatsLocked(source, voteSendUnixNano, voteReceiveAt, hashDuration, statusCheckDuration)

	if _, done := q.completed[requestID]; done {
		q.stats.completedDuplicates++
		return nil, map[string]any{"status": "eo_nested_request_already_dispatched", "request_id": requestID}
	}

	window := q.windows[requestID]
	if window == nil {
		window = &nestedEORequestWindow{
			requestID:   requestID,
			buckets:     make(map[string]*nestedEORequestBucket),
			firstVote:   voteReceiveAt,
			firstSource: source,
		}
		q.windows[requestID] = window
		q.stats.windows++
		if source == q.name {
			q.stats.firstLocal++
		} else {
			q.stats.firstRemote++
		}
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
		q.stats.waitingForQuorum++
		return nil, map[string]any{
			"status":     "eo_nested_request_waiting_for_quorum",
			"request_id": requestID,
			"count":      len(bucket.senders),
			"hash":       hash,
		}
	}
	return q.selectBucketLocked(requestID, window, bucket, false, voteSendUnixNano, voteReceiveAt), map[string]any{
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
			selected = q.selectBucketLocked(requestID, window, bucket, true, 0, time.Now())
		}
	}
	q.mu.Unlock()

	if selected != nil {
		q.dispatchSelected(selected)
	}
}

func (q *NestedEORequestQuorumGate) selectBucketLocked(requestID string, window *nestedEORequestWindow, bucket *nestedEORequestBucket, fallback bool, voteSendUnixNano int64, voteReceiveAt time.Time) *nestedEOSelectedRequest {
	if window.dispatched {
		return nil
	}
	selectedAt := time.Now()
	window.dispatched = true
	if window.timer != nil {
		window.timer.Stop()
	}
	delete(q.windows, requestID)
	q.completed[requestID] = struct{}{}
	q.stats.dispatches++
	if fallback {
		q.stats.fallbacks++
	}
	if !window.firstVote.IsZero() {
		selectWait := selectedAt.Sub(window.firstVote)
		if selectWait >= 0 {
			q.stats.totalFirstVoteToSelect += selectWait
			if selectWait > q.stats.maxFirstVoteToSelect {
				q.stats.maxFirstVoteToSelect = selectWait
			}
		}
		receiveWait := voteReceiveAt.Sub(window.firstVote)
		if receiveWait >= 0 {
			q.stats.secondVoteReceiveCount++
			q.stats.totalFirstVoteToSecondReceive += receiveWait
			if receiveWait > q.stats.maxFirstVoteToSecondReceive {
				q.stats.maxFirstVoteToSecondReceive = receiveWait
			}
		}
		if voteSendUnixNano > 0 {
			sendWait := time.Unix(0, voteSendUnixNano).Sub(window.firstVote)
			if sendWait >= 0 {
				q.stats.secondVoteSendCount++
				q.stats.totalFirstVoteToSecondSend += sendWait
				if sendWait > q.stats.maxFirstVoteToSecondSend {
					q.stats.maxFirstVoteToSecondSend = sendWait
				}
			}
		}
	}
	return &nestedEOSelectedRequest{
		requestID: requestID,
		hash:      bucket.hash,
		request:   cloneMapAny(bucket.request),
		targets:   append([]string{}, bucket.targets...),
		fallback:  fallback,
		variants:  nestedEORequestVariants(window),
	}
}

func (q *NestedEORequestQuorumGate) recordVoteStatsLocked(source string, voteSendUnixNano int64, voteReceiveAt time.Time, hashDuration time.Duration, statusCheckDuration time.Duration) {
	q.stats.votes++
	if source == q.name {
		q.stats.localVotes++
	} else {
		q.stats.remoteVotes++
	}
	q.stats.hashCount++
	q.stats.totalHash += hashDuration
	if hashDuration > q.stats.maxHash {
		q.stats.maxHash = hashDuration
	}
	q.recordStatusCheckStatsLocked(statusCheckDuration)
	if voteSendUnixNano > 0 {
		transit := voteReceiveAt.Sub(time.Unix(0, voteSendUnixNano))
		if transit >= 0 {
			q.stats.remoteTransitCount++
			q.stats.totalRemoteTransit += transit
			if transit > q.stats.maxRemoteTransit {
				q.stats.maxRemoteTransit = transit
			}
		}
	}
}

func (q *NestedEORequestQuorumGate) recordStatusCheckStatsLocked(duration time.Duration) {
	q.stats.statusCheckCount++
	q.stats.totalStatusCheck += duration
	if duration > q.stats.maxStatusCheck {
		q.stats.maxStatusCheck = duration
	}
}

func (q *NestedEORequestQuorumGate) recordLockStatsLocked(lockWait time.Duration, lockedDuration time.Duration) {
	q.stats.lockCount++
	q.stats.totalLockWait += lockWait
	if lockWait > q.stats.maxLockWait {
		q.stats.maxLockWait = lockWait
	}
	q.stats.totalLocked += lockedDuration
	if lockedDuration > q.stats.maxLocked {
		q.stats.maxLocked = lockedDuration
	}
}

func (q *NestedEORequestQuorumGate) recordDispatchSelectedStats(duration time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stats.dispatchSelectedCount++
	q.stats.totalDispatchSelected += duration
	if duration > q.stats.maxDispatchSelected {
		q.stats.maxDispatchSelected = duration
	}
	q.maybeLogStatsLocked(time.Now())
}

func (q *NestedEORequestQuorumGate) maybeLogStatsLocked(now time.Time) {
	if q.stats.windowStart.IsZero() {
		q.stats.windowStart = now
		return
	}
	elapsed := now.Sub(q.stats.windowStart)
	if elapsed < nestedEORequestQuorumStatsWindow {
		return
	}
	log.Printf(
		"%s: nested_eo_request_quorum_breakdown window_ms=%d votes=%d local_votes=%d remote_votes=%d windows=%d first_local=%d first_remote=%d waiting_for_quorum=%d dispatches=%d fallbacks=%d completed_duplicates=%d avg_first_vote_to_second_send_us=%d max_first_vote_to_second_send_us=%d avg_remote_vote_transit_us=%d max_remote_vote_transit_us=%d avg_first_vote_to_second_receive_us=%d max_first_vote_to_second_receive_us=%d avg_first_vote_to_select_us=%d max_first_vote_to_select_us=%d avg_status_check_us=%d max_status_check_us=%d avg_hash_us=%d max_hash_us=%d avg_lock_wait_us=%d max_lock_wait_us=%d avg_locked_us=%d max_locked_us=%d avg_dispatch_selected_us=%d max_dispatch_selected_us=%d open_windows=%d completed=%d",
		q.name,
		elapsed.Milliseconds(),
		q.stats.votes,
		q.stats.localVotes,
		q.stats.remoteVotes,
		q.stats.windows,
		q.stats.firstLocal,
		q.stats.firstRemote,
		q.stats.waitingForQuorum,
		q.stats.dispatches,
		q.stats.fallbacks,
		q.stats.completedDuplicates,
		avgDurationUs(q.stats.totalFirstVoteToSecondSend, q.stats.secondVoteSendCount),
		q.stats.maxFirstVoteToSecondSend.Microseconds(),
		avgDurationUs(q.stats.totalRemoteTransit, q.stats.remoteTransitCount),
		q.stats.maxRemoteTransit.Microseconds(),
		avgDurationUs(q.stats.totalFirstVoteToSecondReceive, q.stats.secondVoteReceiveCount),
		q.stats.maxFirstVoteToSecondReceive.Microseconds(),
		avgDurationUs(q.stats.totalFirstVoteToSelect, q.stats.dispatches),
		q.stats.maxFirstVoteToSelect.Microseconds(),
		avgDurationUs(q.stats.totalStatusCheck, q.stats.statusCheckCount),
		q.stats.maxStatusCheck.Microseconds(),
		avgDurationUs(q.stats.totalHash, q.stats.hashCount),
		q.stats.maxHash.Microseconds(),
		avgDurationUs(q.stats.totalLockWait, q.stats.lockCount),
		q.stats.maxLockWait.Microseconds(),
		avgDurationUs(q.stats.totalLocked, q.stats.lockCount),
		q.stats.maxLocked.Microseconds(),
		avgDurationUs(q.stats.totalDispatchSelected, q.stats.dispatchSelectedCount),
		q.stats.maxDispatchSelected.Microseconds(),
		len(q.windows),
		len(q.completed),
	)
	q.stats = nestedEORequestQuorumStats{windowStart: now}
}

func avgDurationUs(total time.Duration, count int) int64 {
	if count <= 0 {
		return 0
	}
	return total.Microseconds() / int64(count)
}

func (q *NestedEORequestQuorumGate) dispatchSelected(selected *nestedEOSelectedRequest) {
	if selected == nil {
		return
	}
	if selected.fallback {
		log.Printf(
			"%s: warning: nested EO request quorum timeout; using deterministic fallback request_id=%s child=%s timeout=%s selected_hash=%s variants=%s",
			q.name,
			selected.requestID,
			nestedResponseChild(selected.request),
			q.timeout,
			selected.hash,
			selected.variants,
		)
	}
	outgoing := cloneMapAny(selected.request)
	outgoing["sender"] = q.name
	if q.onSelected != nil {
		q.onSelected(selected.requestID)
	}
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

func nestedEORequestBatchVotes(value any) []map[string]any {
	switch typed := value.(type) {
	case []map[string]any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			if item != nil {
				out = append(out, item)
			}
		}
		return out
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			if vote, ok := item.(map[string]any); ok && vote != nil {
				out = append(out, vote)
			}
		}
		return out
	default:
		return nil
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
