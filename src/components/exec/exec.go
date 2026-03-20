package exec

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"aegean/common"
)

// logExecStateDetails gates verbose state-dump logs in exec verification paths
// Keep false in normal runs to avoid very large logs
const logExecStateDetails = false

type pendingExecResult struct {
	outputs    []map[string]any
	state      map[string]string
	merkleRoot string
	token      string
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
)

type ingressEvent struct {
	kind    ingressEventKind
	payload map[string]any
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
	batchBuffer   *common.OOOBuffer[map[string]any]
	verifyBuffer  *common.MultiOOOBuffer[map[string]any]
	nextBatchSeq  int
	nextVerifySeq int
	workerCount   int
	scheduler     *execScheduler
	batchCtx      *batchMerkleContext
	// Request execution hook
	ExecuteRequest ExecuteRequestFunc
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
		workerCount:           common.MustInt(initialRunConfig, "worker_count"),
	}
	exec.verifyResponseQuorum = common.NewQuorumHelper(verifyResponseQuorumSize)
	exec.storeCheckpoint(0, stable.PrevHash, stable.KVStore, stable.MerkleRoot)
	exec.scheduler = newExecScheduler(initialRunConfig)
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
	requestIDRaw, ok := payload["request_id"]
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

func (e *Exec) runCoordinator() {
	for {
		ev := <-e.ingressCh
		e.applyIngressEvent(ev)

		for {
			select {
			case ev = <-e.ingressCh:
				e.applyIngressEvent(ev)
			default:
				if !e.drainBufferedMessages() {
					goto next
				}
			}
		}
	next:
	}
}

func (e *Exec) applyIngressEvent(ev ingressEvent) {
	switch ev.kind {
	case ingressBatchEvent:
		seqNum := common.GetInt(ev.payload, "seq_num")
		e.batchBuffer.Add(seqNum, ev.payload)
	case ingressVerifyResponseEvent:
		seqNum := common.GetInt(ev.payload, "seq_num")
		view := common.GetInt(ev.payload, "view")
		e.verifyBuffer.Add(view, seqNum, ev.payload)
	default:
		return
	}
}

// Only the coordinator goroutine invokes this method; that single-owner model
// provides sequencing safety for nextBatchSeq/nextVerifySeq advancement.
func (e *Exec) drainBufferedMessages() bool {
	bResp := e.flushNextBatch()
	vResp := e.flushNextVerify()
	vrResp := e.flushNextVerifyResponse()
	return bResp || vResp || vrResp
}
