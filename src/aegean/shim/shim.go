package shim

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"aegean/common"
	netx "aegean/net"
	"aegean/telemetry"
)

const nestedTimingMetadataPrefix = "_nested_"
const nestedTraceEnabledKey = "_nested_trace_enabled"
const nestedBackendCommitUnixNanoKey = "_nested_backend_commit_unix_nano"
const nestedBackendNodeKey = "_nested_backend_node"
const nestedBackendSeqNumKey = "_nested_backend_seq_num"
const nestedBackendBatchOutputCountKey = "_nested_backend_batch_output_count"
const nestedParentShimReceiveUnixNanoKey = "_nested_parent_shim_receive_unix_nano"
const nestedParentQuorumUnixNanoKey = "_nested_parent_quorum_unix_nano"
const nestedChildShimFirstReceiveUnixNanoKey = "_nested_child_shim_first_receive_unix_nano"
const nestedChildShimQuorumUnixNanoKey = "_nested_child_shim_quorum_unix_nano"
const nestedChildShimHandleStartUnixNanoKey = "_nested_child_shim_handle_start_unix_nano"
const nestedChildShimEnqueueUnixNanoKey = "_nested_child_shim_enqueue_unix_nano"
const nestedChildShimSendStartUnixNanoKey = "_nested_child_shim_send_start_unix_nano"

const defaultShimFanoutQueueSize = 8192

type Shim struct {
	Name                 string
	BatcherCh            chan<- map[string]any
	ExecCh               chan<- map[string]any
	Clients              []string
	Peers                []string
	isPrimaryBatcher     bool
	requestQuorumHelper  *common.QuorumHelper
	responseQuorumHelper *common.QuorumHelper
	nestedReturnMu       sync.Mutex
	nestedReturnStats    map[string]*nestedReturnBatchStats
	nestedRequestMu      sync.Mutex
	nestedRequestFirst   map[string]int64
	fanoutQueues         map[string]chan map[string]any
}

type nestedReturnBatchStats struct {
	backendNode          string
	backendSeqNum        int
	expected             int
	expectedSends        int
	seen                 int
	enqueued             int
	sends                int
	commitToShimStart    time.Duration
	maxCommitToShimStart time.Duration
	shimHandle           time.Duration
	maxShimHandle        time.Duration
	enqueueWait          time.Duration
	maxEnqueueWait       time.Duration
	queueDelay           time.Duration
	maxQueueDelay        time.Duration
	sendTotal            time.Duration
	maxSend              time.Duration
	sendErrors           int
	recipients           map[string]*nestedRecipientReturnStats
}

type nestedRecipientReturnStats struct {
	count int
	sum   time.Duration
	max   time.Duration
	errs  int
}

func NewShim(name string, batcherCh chan<- map[string]any, execCh chan<- map[string]any, clients []string, peers []string, isPrimaryBatcher bool, quorumSize int) *Shim {
	if batcherCh == nil {
		panic("shim component requires non-nil batcherCh")
	}
	shim := &Shim{
		Name:                name,
		BatcherCh:           batcherCh,
		ExecCh:              execCh,
		Clients:             clients,
		Peers:               peers,
		isPrimaryBatcher:    isPrimaryBatcher,
		requestQuorumHelper: common.NewQuorumHelper(quorumSize),
		// TODO: quorumSize should depend on size of nested service
		responseQuorumHelper: common.NewQuorumHelper(quorumSize),
		nestedReturnStats:    make(map[string]*nestedReturnBatchStats),
		nestedRequestFirst:   make(map[string]int64),
		fanoutQueues:         make(map[string]chan map[string]any),
	}
	shim.startFanoutWorkers()
	return shim
}

func (s *Shim) HandleRequestMessage(payload map[string]any) map[string]any {
	// Handle incoming client request - wait for quorum then forward
	requestID := payload["request_id"]
	sender, _ := payload["sender"].(string)
	if !s.isPrimaryBatcher {
		return map[string]any{"status": "ignored_non_primary"}
	}

	firstReceiveUnixNano := int64(0)
	if nestedTraceEnabled(payload) {
		firstReceiveUnixNano = s.recordNestedRequestReceive(requestID, time.Now().UnixNano())
	}
	if !s.requestQuorumHelper.Add(requestID, sender) {
		return map[string]any{"status": "waiting_for_quorum"}
	}
	if nestedTraceEnabled(payload) {
		payload[nestedChildShimFirstReceiveUnixNanoKey] = firstReceiveUnixNano
		payload[nestedChildShimQuorumUnixNanoKey] = time.Now().UnixNano()
	}
	if s.BatcherCh != nil {
		s.BatcherCh <- payload
	}
	return map[string]any{"status": "forwarded_to_mid_execs"}
}

func (s *Shim) recordNestedRequestReceive(requestID any, receiveUnixNano int64) int64 {
	key := fmt.Sprintf("%v", requestID)
	s.nestedRequestMu.Lock()
	defer s.nestedRequestMu.Unlock()
	if first, ok := s.nestedRequestFirst[key]; ok && first > 0 {
		return first
	}
	s.nestedRequestFirst[key] = receiveUnixNano
	return receiveUnixNano
}

func (s *Shim) HandleIncomingResponse(payload map[string]any) map[string]any {
	if nestedTraceEnabled(payload) {
		payload[nestedParentShimReceiveUnixNanoKey] = time.Now().UnixNano()
	}

	requestID := payload["request_id"]
	sender, _ := payload["sender"].(string)
	if sender == "" {
		return map[string]any{"status": "error", "error": "missing sender"}
	}

	if !s.responseQuorumHelper.Add(requestID, sender) {
		return map[string]any{"status": "waiting_for_quorum", "request_id": requestID}
	}

	// This assumes nested responses are equivalent across backends
	payload["shim_quorum_aggregated"] = true

	if s.ExecCh != nil {
		if nestedTraceEnabled(payload) {
			payload[nestedParentQuorumUnixNanoKey] = time.Now().UnixNano()
		}
		s.ExecCh <- payload
		return map[string]any{"status": "forwarded_nested_response", "request_id": requestID}
	}
	return map[string]any{"status": "error", "error": "exec channel not configured", "request_id": requestID}
}

func (s *Shim) HandleOutgoingResponse(payload map[string]any) map[string]any {
	startedAt := time.Now()
	requestID := payload["request_id"]
	responseData, _ := payload["response"].(map[string]any)
	sender := s.Name

	fullResponse := map[string]any{
		"type":                   "response",
		"request_id":             requestID,
		"response":               responseData,
		"sender":                 sender,
		"shim_quorum_aggregated": true,
	}
	if parentRequestID, ok := payload["parent_request_id"]; ok && parentRequestID != nil {
		fullResponse["parent_request_id"] = parentRequestID
	}
	for key, value := range payload {
		if strings.HasPrefix(key, nestedTimingMetadataPrefix) {
			fullResponse[key] = value
		}
	}
	telemetry.CopyContext(fullResponse, payload)

	// Handle response from exec - broadcast to all clients that sent the request
	// TODO: Or do we wait for a quorum, and then broadcast

	traceNested := nestedTraceEnabled(payload)
	if traceNested {
		fullResponse[nestedChildShimHandleStartUnixNanoKey] = startedAt.UnixNano()
		s.recordNestedReturnStart(payload, startedAt)
	}
	enqueueWaitTotal := time.Duration(0)
	maxEnqueueWait := time.Duration(0)
	for _, client := range s.Clients {
		outgoing := copyMapAny(fullResponse)
		if traceNested {
			outgoing[nestedChildShimEnqueueUnixNanoKey] = time.Now().UnixNano()
		}
		enqueueStart := time.Now()
		s.enqueueFanout(client, outgoing)
		enqueueWait := time.Since(enqueueStart)
		enqueueWaitTotal += enqueueWait
		if enqueueWait > maxEnqueueWait {
			maxEnqueueWait = enqueueWait
		}
	}
	if traceNested {
		s.recordNestedReturnEnqueued(payload, time.Since(startedAt), enqueueWaitTotal, maxEnqueueWait)
	}

	return map[string]any{"status": "response_broadcast", "recipients": s.Clients}
}

func (s *Shim) startFanoutWorkers() {
	for _, client := range s.Clients {
		if _, exists := s.fanoutQueues[client]; exists {
			continue
		}
		queue := make(chan map[string]any, defaultShimFanoutQueueSize)
		s.fanoutQueues[client] = queue
		go s.runFanoutWorker(client, queue)
	}
}

func (s *Shim) enqueueFanout(client string, payload map[string]any) {
	queue := s.fanoutQueues[client]
	if queue == nil {
		_, _ = netx.SendMessage(client, 8000, payload)
		return
	}
	queue <- payload
}

func (s *Shim) runFanoutWorker(client string, queue <-chan map[string]any) {
	for payload := range queue {
		dequeuedAt := time.Now()
		queueDelay := time.Duration(0)
		if enqueueUnixNano := nestedInt64(payload[nestedChildShimEnqueueUnixNanoKey]); enqueueUnixNano > 0 {
			queueDelay = dequeuedAt.Sub(time.Unix(0, enqueueUnixNano))
			if queueDelay < 0 {
				queueDelay = 0
			}
		}
		traceNested := nestedTraceEnabled(payload)
		sendStart := time.Now()
		if traceNested {
			payload[nestedChildShimSendStartUnixNanoKey] = sendStart.UnixNano()
		}
		_, err := netx.SendMessage(client, 8000, payload)
		sendDuration := time.Since(sendStart)
		if traceNested {
			s.recordNestedReturnSend(payload, client, queueDelay, sendDuration, err != nil)
		}
	}
}

func (s *Shim) recordNestedReturnStart(payload map[string]any, startedAt time.Time) {
	backendNode, _ := payload[nestedBackendNodeKey].(string)
	if backendNode == "" {
		backendNode = s.Name
	}
	backendSeqNum := nestedInt(payload[nestedBackendSeqNumKey])
	if backendSeqNum <= 0 {
		return
	}
	expected := nestedInt(payload[nestedBackendBatchOutputCountKey])
	if expected <= 0 {
		expected = 1
	}
	key := fmt.Sprintf("%s/%d", backendNode, backendSeqNum)

	var commitToShimStart time.Duration
	if commitUnixNano := nestedInt64(payload[nestedBackendCommitUnixNanoKey]); commitUnixNano > 0 {
		commitToShimStart = startedAt.Sub(time.Unix(0, commitUnixNano))
		if commitToShimStart < 0 {
			commitToShimStart = 0
		}
	}

	s.nestedReturnMu.Lock()
	stats := s.nestedReturnStats[key]
	if stats == nil {
		stats = &nestedReturnBatchStats{
			backendNode:   backendNode,
			backendSeqNum: backendSeqNum,
			expected:      expected,
			expectedSends: expected * len(s.Clients),
			recipients:    make(map[string]*nestedRecipientReturnStats),
		}
		s.nestedReturnStats[key] = stats
	} else if stats.expectedSends == 0 {
		stats.expectedSends = stats.expected * len(s.Clients)
	}
	stats.seen++
	stats.commitToShimStart += commitToShimStart
	if commitToShimStart > stats.maxCommitToShimStart {
		stats.maxCommitToShimStart = commitToShimStart
	}
	s.nestedReturnMu.Unlock()
}

func (s *Shim) recordNestedReturnEnqueued(payload map[string]any, handleDuration time.Duration, enqueueWaitTotal time.Duration, maxEnqueueWait time.Duration) {
	key := s.nestedReturnKey(payload)
	if key == "" {
		return
	}

	var toLog *nestedReturnBatchStats
	s.nestedReturnMu.Lock()
	stats := s.nestedReturnStats[key]
	if stats == nil {
		s.nestedReturnMu.Unlock()
		return
	}
	stats.enqueued++
	stats.shimHandle += handleDuration
	if handleDuration > stats.maxShimHandle {
		stats.maxShimHandle = handleDuration
	}
	stats.enqueueWait += enqueueWaitTotal
	if maxEnqueueWait > stats.maxEnqueueWait {
		stats.maxEnqueueWait = maxEnqueueWait
	}
	if s.nestedReturnCompleteLocked(stats) {
		toLog = stats
		delete(s.nestedReturnStats, key)
	}
	s.nestedReturnMu.Unlock()

	if toLog == nil {
		return
	}
	s.logNestedReturnTiming(toLog)
}

func (s *Shim) recordNestedReturnSend(payload map[string]any, recipient string, queueDelay time.Duration, sendDuration time.Duration, sendError bool) {
	key := s.nestedReturnKey(payload)
	if key == "" {
		return
	}

	var toLog *nestedReturnBatchStats
	s.nestedReturnMu.Lock()
	stats := s.nestedReturnStats[key]
	if stats == nil {
		s.nestedReturnMu.Unlock()
		return
	}
	stats.sends++
	stats.queueDelay += queueDelay
	if queueDelay > stats.maxQueueDelay {
		stats.maxQueueDelay = queueDelay
	}
	stats.sendTotal += sendDuration
	if sendDuration > stats.maxSend {
		stats.maxSend = sendDuration
	}
	if sendError {
		stats.sendErrors++
	}
	recipientStats := stats.recipients[recipient]
	if recipientStats == nil {
		recipientStats = &nestedRecipientReturnStats{}
		stats.recipients[recipient] = recipientStats
	}
	recipientStats.count++
	recipientStats.sum += sendDuration
	if sendDuration > recipientStats.max {
		recipientStats.max = sendDuration
	}
	if sendError {
		recipientStats.errs++
	}
	if s.nestedReturnCompleteLocked(stats) {
		toLog = stats
		delete(s.nestedReturnStats, key)
	}
	s.nestedReturnMu.Unlock()

	if toLog == nil {
		return
	}
	s.logNestedReturnTiming(toLog)
}

func (s *Shim) nestedReturnCompleteLocked(stats *nestedReturnBatchStats) bool {
	if stats == nil {
		return false
	}
	return stats.seen >= stats.expected && stats.enqueued >= stats.expected && stats.sends >= stats.expectedSends
}

func (s *Shim) logNestedReturnTiming(stats *nestedReturnBatchStats) {
	log.Printf(
		"%s: nested_child_return_timing backend_node=%s backend_seq_num=%d response_count=%d expected_count=%d send_count=%d expected_send_count=%d commit_to_shim_start_us=%d max_commit_to_shim_start_us=%d shim_handle_us=%d max_shim_handle_us=%d enqueue_wait_us=%d max_enqueue_wait_us=%d queue_delay_us=%d max_queue_delay_us=%d send_total_us=%d max_send_us=%d send_errors=%d recipients=%s",
		s.Name,
		stats.backendNode,
		stats.backendSeqNum,
		stats.seen,
		stats.expected,
		stats.sends,
		stats.expectedSends,
		stats.commitToShimStart.Microseconds(),
		stats.maxCommitToShimStart.Microseconds(),
		stats.shimHandle.Microseconds(),
		stats.maxShimHandle.Microseconds(),
		stats.enqueueWait.Microseconds(),
		stats.maxEnqueueWait.Microseconds(),
		stats.queueDelay.Microseconds(),
		stats.maxQueueDelay.Microseconds(),
		stats.sendTotal.Microseconds(),
		stats.maxSend.Microseconds(),
		stats.sendErrors,
		formatNestedRecipientStats(stats.recipients),
	)
}

func (s *Shim) nestedReturnKey(payload map[string]any) string {
	backendNode, _ := payload[nestedBackendNodeKey].(string)
	if backendNode == "" {
		backendNode = s.Name
	}
	backendSeqNum := nestedInt(payload[nestedBackendSeqNumKey])
	if backendSeqNum <= 0 {
		return ""
	}
	return fmt.Sprintf("%s/%d", backendNode, backendSeqNum)
}

func formatNestedRecipientStats(recipients map[string]*nestedRecipientReturnStats) string {
	if len(recipients) == 0 {
		return "[]"
	}
	keys := make([]string, 0, len(recipients))
	for key := range recipients {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		stats := recipients[key]
		if stats == nil {
			continue
		}
		parts = append(parts, fmt.Sprintf(
			"%s:%d:%d:%d:%d",
			key,
			stats.count,
			stats.sum.Microseconds(),
			stats.max.Microseconds(),
			stats.errs,
		))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func nestedTraceEnabled(payload map[string]any) bool {
	if payload == nil {
		return false
	}
	enabled, _ := payload[nestedTraceEnabledKey].(bool)
	return enabled
}

func nestedInt(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
}

func nestedInt64(value any) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case float64:
		return int64(typed)
	default:
		return 0
	}
}

func copyMapAny(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	out := make(map[string]any, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}
