package exec

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"aegean/common"
	"aegean/telemetry"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// logExecStateDetails gates verbose state-dump logs in exec verification paths
// Keep false in normal runs to avoid very large logs
const logExecStateDetails = false

const batchBufferWaitSpanContextKey = "batch_buffer_wait_span"
const requestVerifyGateWaitSpanContextKey = "request_verify_gate_wait_span"
const requestVerifyWaitSpanContextKey = "request_verify_wait_span"
const postNestedVerifyGateWaitSpanContextKey = "post_nested_verify_gate_wait_span"
const requestBatchSeqContextKey = "request_batch_seq"

type pendingExecResult struct {
	outputs    []map[string]any
	state      map[string]string
	merkleRoot string
	token      string
	verifySpan trace.Span
	// verifySent indicates whether a verify message has been sent for this seq
	verifySent bool
}

type batchMerkleContext struct {
	baseKeys   map[string]struct{}
	pendingNew map[string]string
}

type ingressEventKind int

const (
	ingressBatchEvent ingressEventKind = iota
	ingressVerifyResponseEvent
	ingressBatchExecutionCompleteEvent
)

type ingressEvent struct {
	kind        ingressEventKind
	payload     map[string]any
	batchResult *batchExecutionResult
}

type batchExecutionTask struct {
	payload map[string]any
}

type batchExecutionResult struct {
	seqNum           int
	payload          map[string]any
	outputs          []map[string]any
	state            map[string]string
	merkleRoot       string
	batchServiceSpan trace.Span
}

type coordinatorStats struct {
	windowStart               time.Time
	ingressEvents             int
	batchEvents               int
	verifyResponseEvents      int
	batchExecutionCompletions int
	drainCalls                int
	drainProgressCalls        int
	flushBatchHits            int
	flushVerifyHits           int
	flushVerifyRespHits       int
	drainWall                 time.Duration
	maxDrainWall              time.Duration
}

// ExecuteRequestFunc handles a single request for an exec node.
type ExecuteRequestFunc func(e *Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any
type InitStateFunc func(e *Exec) map[string]string

type Exec struct {
	Name      string
	Verifiers []string
	Peers     []string
	RunConfig map[string]any
	// Local component channels
	VerifierCh chan<- map[string]any
	ShimCh     chan<- map[string]any
	nestedEO   NestedEOReplicator
	// mu protects protocol/control-plane fields:
	// stableState, pendingExecResults, forceSequential, view, checkpoints,
	// verify-response quorum/timers/maps, nextBatchSeq, nextVerifySeq
	mu sync.Mutex
	// processMu is a maintenance gate for tests/transfer windows.
	// Sequencing is owned by the coordinator goroutine, not by this lock.
	processMu sync.Mutex
	// ingressCh is the single-owner coordinator queue.
	// All buffer enqueue operations flow through this channel so enqueue and
	// buffer clear/reset transitions remain serialized.
	ingressCh chan ingressEvent
	// stateMu protects workingState and batchCtx while executing requests
	// Preferred lock order when both are needed: mu -> stateMu
	stateMu sync.RWMutex
	// State management for rollback
	stableState  State
	workingState State
	// Buffers tentative execution results until verifiers confirm, then either commits or discards them
	// keys: outputs, state, merkleRoot, token, verifySent
	pendingExecResults map[int]pendingExecResult
	// Stores original inputs so the exec can deterministically replay later if needed
	// maps seq_num to batch payload object
	replayableBatchInputs map[int]map[string]any
	// Sequential execution flag (set after rollback)
	forceSequential bool
	// Current execution view.
	view int
	// Quorum + dedupe for verify responses from verifiers.
	verifyResponseQuorum *common.QuorumHelper
	verifyResponseBySeq  map[int]map[string]struct{}
	// Checkpoints for rollback to agreed (n, T).
	checkpoints map[int]rollbackCheckpoint
	// Timeout for unresolved verifier responses.
	verifyResponseTimeout time.Duration
	verifyResponseTimers  map[int]*time.Timer
	// Out-of-order buffers
	batchBuffer           *common.OOOBuffer[map[string]any]
	verifyBuffer          *common.MultiOOOBuffer[map[string]any]
	nextBatchSeq          int
	nextVerifySeq         int
	batchExecuting        bool
	workerCount           int
	scheduler             *execScheduler
	batchCtx              *batchMerkleContext
	batchExecCh           chan batchExecutionTask
	coordStats            coordinatorStats
	nestedDispatchTracker *nestedDispatchTracker
	// Request execution hook
	ExecuteRequest ExecuteRequestFunc
}

func (e *Exec) GetRunConfig() map[string]any {
	return e.RunConfig
}

func NewExec(name string, verifiers []string, peers []string, verifierCh chan<- map[string]any, shimCh chan<- map[string]any, verifyResponseQuorumSize int, executeRequest ExecuteRequestFunc, initStateFn InitStateFunc, runConfig map[string]any) *Exec {
	if verifierCh == nil || shimCh == nil {
		panic("exec component requires non-nil channels")
	}
	if executeRequest == nil {
		panic("exec component requires ExecuteRequest")
	}

	initialRunConfig := runConfig

	initialKV := map[string]string{}
	if initStateFn != nil {
		if customKV := initStateFn(&Exec{Name: name, RunConfig: initialRunConfig}); customKV != nil {
			initialKV = common.CopyStringMap(customKV)
		}
	}
	initialMerkle := NewMerkleTreeFromMap(initialKV)
	stable := State{
		KVStore:    common.CopyStringMap(initialKV),
		Merkle:     initialMerkle.Clone(),
		MerkleRoot: initialMerkle.Root(),
		SeqNum:     0,
		PrevHash:   strings.Repeat("0", 64),
		Verified:   true,
	}
	working := State{
		KVStore:    common.CopyStringMap(initialKV),
		Merkle:     initialMerkle.Clone(),
		MerkleRoot: initialMerkle.Root(),
		SeqNum:     0,
		PrevHash:   stable.PrevHash,
		Verified:   false,
	}
	exec := &Exec{
		Name:                  name,
		Verifiers:             verifiers,
		Peers:                 peers,
		RunConfig:             initialRunConfig,
		VerifierCh:            verifierCh,
		ShimCh:                shimCh,
		ExecuteRequest:        executeRequest,
		stableState:           stable,
		workingState:          working,
		pendingExecResults:    make(map[int]pendingExecResult),
		replayableBatchInputs: make(map[int]map[string]any),
		view:                  1,
		verifyResponseBySeq:   make(map[int]map[string]struct{}),
		checkpoints:           make(map[int]rollbackCheckpoint),
		verifyResponseTimeout: 2 * time.Second,
		verifyResponseTimers:  make(map[int]*time.Timer),
		batchBuffer:           common.NewOOOBuffer[map[string]any](),
		verifyBuffer:          common.NewMultiOOOBuffer[map[string]any](),
		nextBatchSeq:          1,
		nextVerifySeq:         1,
		ingressCh:             make(chan ingressEvent, 8192),
		batchExecCh:           make(chan batchExecutionTask, 1),
		workerCount:           common.MustInt(initialRunConfig, "worker_count"),
		coordStats:            coordinatorStats{windowStart: time.Now()},
		nestedDispatchTracker: newNestedDispatchTracker(),
	}
	exec.verifyResponseQuorum = common.NewQuorumHelper(verifyResponseQuorumSize)
	exec.storeCheckpoint(0, stable.PrevHash, stable.KVStore, stable.MerkleRoot)
	exec.scheduler = newExecScheduler(initialRunConfig)
	go exec.runBatchExecutor()
	go exec.runCoordinator()
	return exec
}

func responseTupleKey(view int, seqNum int, token string, forceSequential bool) string {
	return fmt.Sprintf("%d|%d|%s|%t", view, seqNum, token, forceSequential)
}

func (e *Exec) ReadKV(key string) string {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	e.workingState.EnsureMerkle()
	if e.batchCtx != nil {
		if value, ok := e.batchCtx.pendingNew[key]; ok {
			return value
		}
	}
	return e.workingState.Merkle.Get(key)
}

func (e *Exec) WriteKV(key, value string) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	e.workingState.EnsureMerkle()
	if e.batchCtx != nil {
		if _, ok := e.batchCtx.baseKeys[key]; !ok {
			// Defer insertion of newly created keys to batch end and insert deterministically.
			e.batchCtx.pendingNew[key] = value
			return
		}
	}
	e.workingState.KVStore[key] = value
	e.workingState.Merkle.Set(key, value)
	e.workingState.MerkleRoot = e.workingState.Merkle.Root()
}

func (e *Exec) beginBatchMerkleContext() {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	e.workingState.EnsureMerkle()
	baseKeys := make(map[string]struct{}, len(e.workingState.KVStore))
	for key := range e.workingState.KVStore {
		baseKeys[key] = struct{}{}
	}
	e.batchCtx = &batchMerkleContext{
		baseKeys:   baseKeys,
		pendingNew: make(map[string]string),
	}
}

func (e *Exec) finalizeBatchMerkleContext() {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if e.batchCtx == nil {
		return
	}
	if len(e.batchCtx.pendingNew) > 0 {
		keys := make([]string, 0, len(e.batchCtx.pendingNew))
		for key := range e.batchCtx.pendingNew {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			value := e.batchCtx.pendingNew[key]
			e.workingState.KVStore[key] = value
			e.workingState.Merkle.Set(key, value)
		}
	}
	e.workingState.MerkleRoot = e.workingState.Merkle.Root()
	e.batchCtx = nil
}

func (e *Exec) BufferNestedResponse(payload map[string]any) bool {
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
	return e.scheduler.enqueueNestedResponse(requestID, payload)
}

func (e *Exec) GetNestedResponses(requestID any) ([]map[string]any, bool) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return nil, false
	}
	return e.scheduler.getNestedResponses(canonicalID)
}

func (e *Exec) HandleBatchMessage(payload map[string]any) map[string]any {
	seqNum := common.GetInt(payload, "seq_num")
	e.ingressCh <- ingressEvent{kind: ingressBatchEvent, payload: payload}
	return map[string]any{"status": "buffered", "seq_num": seqNum}
}

func (e *Exec) HandleVerifyResponseMessage(payload map[string]any) map[string]any {
	seqNum := common.GetInt(payload, "seq_num")
	view := common.GetInt(payload, "view")
	e.ingressCh <- ingressEvent{kind: ingressVerifyResponseEvent, payload: payload}
	return map[string]any{"status": "buffered", "view": view, "seq_num": seqNum}
}

func (e *Exec) HandleStateTransferRequestMessage(payload map[string]any) map[string]any {
	return e.handleStateTransferRequest(payload)
}

func (e *Exec) runBatchExecutor() {
	for task := range e.batchExecCh {
		result := e.executeBatch(task.payload)
		e.ingressCh <- ingressEvent{kind: ingressBatchExecutionCompleteEvent, batchResult: result}
	}
}

func (e *Exec) endRequestDispatchWait(request map[string]any) {
	_ = request
}

func (e *Exec) startRequestSpan(request map[string]any, contextKey string, spanName string, attrs ...attribute.KeyValue) {
	requestID, ok := canonicalRequestID(request["request_id"])
	if !ok {
		return
	}
	if _, exists := e.GetRequestContextValue(requestID, contextKey); exists {
		return
	}
	_, span := telemetry.StartSpanFromPayload(
		request,
		spanName,
		append(
			append(
				telemetry.AttrsFromPayload(request),
				attribute.String("node.name", e.Name),
			),
			attrs...,
		)...,
	)
	_ = e.SetRequestContextValue(requestID, contextKey, span)
}

func (e *Exec) endRequestSpan(requestID any, contextKey string) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return
	}
	spanAny, exists := e.GetRequestContextValue(canonicalID, contextKey)
	if !exists {
		return
	}
	if span, ok := spanAny.(trace.Span); ok && span != nil {
		span.End()
	}
	e.DeleteRequestContextValue(canonicalID, contextKey)
}

func (e *Exec) requestPayloadsForSeq(seqNum int) []map[string]any {
	e.mu.Lock()
	batchPayload, ok := e.replayableBatchInputs[seqNum]
	e.mu.Unlock()
	if !ok {
		return nil
	}
	parallelBatches, ok := batchPayload["parallel_batches"].([][]map[string]any)
	if !ok {
		return nil
	}
	requests := make([]map[string]any, 0)
	for _, batch := range parallelBatches {
		requests = append(requests, batch...)
	}
	return requests
}

func (e *Exec) requestPayloadForSeq(seqNum int, requestID any) map[string]any {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return nil
	}
	for _, request := range e.requestPayloadsForSeq(seqNum) {
		reqID, ok := canonicalRequestID(request["request_id"])
		if ok && reqID == canonicalID {
			return request
		}
	}
	return nil
}

func PostNestedVerifyGateWaitSpanKey() string {
	return postNestedVerifyGateWaitSpanContextKey
}

func (e *Exec) StartRequestWaitSpan(request map[string]any, contextKey string, spanName string, attrs ...attribute.KeyValue) {
	e.startRequestSpan(request, contextKey, spanName, attrs...)
}

func (e *Exec) RequestVerifyGateSnapshot() (int, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.nextVerifySeq, e.stableState.SeqNum
}

func (e *Exec) runCoordinator() {
	for {
		ev := <-e.ingressCh
		e.applyIngressEvent(ev)
		e.maybeLogCoordinatorStats()

		for {
			select {
			case ev = <-e.ingressCh:
				e.applyIngressEvent(ev)
				e.maybeLogCoordinatorStats()
			default:
				if !e.drainBufferedMessages() {
					goto next
				}
				e.maybeLogCoordinatorStats()
			}
		}
	next:
	}
}

func (e *Exec) applyIngressEvent(ev ingressEvent) {
	e.coordStats.ingressEvents++
	switch ev.kind {
	case ingressBatchEvent:
		e.coordStats.batchEvents++
		seqNum := common.GetInt(ev.payload, "seq_num")
		e.mu.Lock()
		nextBatchSeq := e.nextBatchSeq
		stableSeq := e.stableState.SeqNum
		batchExecuting := e.batchExecuting
		e.mu.Unlock()
		if parallelBatches, ok := ev.payload["parallel_batches"].([][]map[string]any); ok {
			requestCount := batchRequestCount(parallelBatches)
			queueDepth := seqNum - nextBatchSeq
			if queueDepth < 0 {
				queueDepth = 0
			}
			_, batchQueueSpan := telemetry.StartSpanFromPayload(
				ev.payload,
				"exec.batch_queue_wait",
				append(
					telemetry.AttrsFromPayload(ev.payload),
					attribute.String("node.name", e.Name),
					attribute.Int("batch.seq_num", seqNum),
					attribute.Int("batch.request_count", requestCount),
					attribute.Int("parallel_batch.count", len(parallelBatches)),
					attribute.Int("batch.queue_depth_on_arrival", queueDepth),
					attribute.Int("batch.next_batch_seq_on_arrival", nextBatchSeq),
					attribute.Int("batch.stable_seq_on_arrival", stableSeq),
					attribute.Bool("batch.executing_on_arrival", batchExecuting),
				)...,
			)
			ev.payload["_batch_queue_wait_span"] = batchQueueSpan
			for parallelBatchIdx, batch := range parallelBatches {
				for requestIdx, request := range batch {
					e.startRequestSpan(
						request,
						batchBufferWaitSpanContextKey,
						"exec.batch_buffer_wait",
						attribute.Int("batch.seq_num", seqNum),
						attribute.Int("batch.request_count", requestCount),
						attribute.Int("parallel_batch.index", parallelBatchIdx),
						attribute.Int("parallel_batch.size", len(batch)),
						attribute.Int("parallel_batch.request_index", requestIdx),
						attribute.Int("parallel_batch.count", len(parallelBatches)),
					)
				}
			}
		}
		e.batchBuffer.Add(seqNum, ev.payload)
	case ingressVerifyResponseEvent:
		e.coordStats.verifyResponseEvents++
		seqNum := common.GetInt(ev.payload, "seq_num")
		view := common.GetInt(ev.payload, "view")
		e.verifyBuffer.Add(view, seqNum, ev.payload)
	case ingressBatchExecutionCompleteEvent:
		e.coordStats.batchExecutionCompletions++
		e.applyBatchExecutionResult(ev.batchResult)
	default:
		return
	}
}

// Only the coordinator goroutine invokes this method; that single-owner model
// provides sequencing safety for nextBatchSeq/nextVerifySeq advancement.
func (e *Exec) drainBufferedMessages() bool {
	start := time.Now()
	bResp := e.flushNextBatch()
	vResp := e.flushNextVerify()
	vrResp := e.flushNextVerifyResponse()
	elapsed := time.Since(start)
	e.coordStats.drainCalls++
	e.coordStats.drainWall += elapsed
	if elapsed > e.coordStats.maxDrainWall {
		e.coordStats.maxDrainWall = elapsed
	}
	if bResp || vResp || vrResp {
		e.coordStats.drainProgressCalls++
	}
	if bResp {
		e.coordStats.flushBatchHits++
	}
	if vResp {
		e.coordStats.flushVerifyHits++
	}
	if vrResp {
		e.coordStats.flushVerifyRespHits++
	}
	return bResp || vResp || vrResp
}

func (e *Exec) maybeLogCoordinatorStats() {
	now := time.Now()
	if now.Sub(e.coordStats.windowStart) < time.Second {
		return
	}
	avgDrainUs := 0.0
	if e.coordStats.drainCalls > 0 {
		avgDrainUs = float64(e.coordStats.drainWall.Microseconds()) / float64(e.coordStats.drainCalls)
	}
	e.mu.Lock()
	nextBatchSeq := e.nextBatchSeq
	nextVerifySeq := e.nextVerifySeq
	stableSeq := e.stableState.SeqNum
	batchExecuting := e.batchExecuting
	e.mu.Unlock()
	log.Printf(
		"%s: coordinator_stats window=%s ingress=%d batch_events=%d verify_resp_events=%d batch_exec_done=%d drain_calls=%d drain_progress=%d flush_batch=%d flush_verify=%d flush_verify_resp=%d avg_drain_us=%.1f max_drain_us=%d next_batch_seq=%d next_verify_seq=%d stable_seq=%d batch_executing=%t",
		e.Name,
		now.Sub(e.coordStats.windowStart).Truncate(time.Millisecond),
		e.coordStats.ingressEvents,
		e.coordStats.batchEvents,
		e.coordStats.verifyResponseEvents,
		e.coordStats.batchExecutionCompletions,
		e.coordStats.drainCalls,
		e.coordStats.drainProgressCalls,
		e.coordStats.flushBatchHits,
		e.coordStats.flushVerifyHits,
		e.coordStats.flushVerifyRespHits,
		avgDrainUs,
		e.coordStats.maxDrainWall.Microseconds(),
		nextBatchSeq,
		nextVerifySeq,
		stableSeq,
		batchExecuting,
	)
	e.coordStats = coordinatorStats{windowStart: now}
}
