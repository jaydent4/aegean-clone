package exec

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"aegean/common"
)

type pendingResponse struct {
	outputs    []map[string]any
	state      map[string]string
	merkle     *MerkleTree
	merkleRoot string
	token      string
	// verifySent indicates whether a verify message has been sent for this seq
	verifySent bool
}

type batchMerkleContext struct {
	baseKeys   map[string]struct{}
	pendingNew map[string]string
}

// ExecuteRequestFunc handles a single request for an exec node.
type ExecuteRequestFunc func(e *Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any

type Exec struct {
	Name      string
	Verifiers []string
	Peers     []string
	// Local component channels
	VerifierCh chan<- map[string]any
	ShimCh     chan<- map[string]any
	// mu protects protocol/control-plane fields:
	// stableState, pendingResponses, forceSequential, view, checkpoints,
	// verify-response quorum/timers/maps, nextBatchSeq, nextVerifySeq
	mu sync.Mutex
	// processMu serializes flush pipelines
	// (HandleBatchMessage/HandleVerifyResponseMessage) so sequence pointers
	// advance coherently
	// Never hold processMu while waiting on remote network I/O
	processMu sync.Mutex
	// stateMu protects workingState and batchCtx while executing requests
	// Preferred lock order when both are needed: mu -> stateMu
	stateMu sync.RWMutex
	// State management for rollback
	stableState  State
	workingState State
	// Pending responses (held until commit)
	pendingResponses map[int]pendingResponse
	// Sequential execution flag (set after rollback)
	forceSequential bool
	// Hard-coded fault parameters for now.
	u int
	r int
	// Current execution view.
	view int
	// Quorum + dedupe for verify responses from verifiers.
	verifyResponseQuorum *common.QuorumHelper
	verifyResponseMsgs   map[string]map[string]any // response tuple key -> payload
	verifyResponseBySeq  map[int]map[string]struct{}
	// Checkpoints for rollback to agreed (n, T).
	checkpoints map[int]rollbackCheckpoint
	// Timeout for unresolved verifier responses.
	verifyResponseTimeout time.Duration
	verifyResponseTimers  map[int]*time.Timer
	// Out-of-order buffers
	batchBuffer   *common.OOOBuffer[map[string]any]
	verifyBuffer  *common.OOOBuffer[map[string]any]
	nextBatchSeq  int
	nextVerifySeq int
	// recoveringTransfers tracks seq numbers currently blocked on state transfer
	// so flushNextVerify can retry recovery without advancing sequence pointers.
	recoveringTransfers map[int]struct{}
	workerCount   int
	scheduler     *execScheduler
	batchCtx      *batchMerkleContext
	// Request execution hook
	ExecuteRequest ExecuteRequestFunc
}

// TODO: request pipelining, parallel pipelining
func NewExec(name string, verifiers []string, peers []string, verifierCh chan<- map[string]any, shimCh chan<- map[string]any, executeRequest ExecuteRequestFunc) *Exec {
	if verifierCh == nil || shimCh == nil {
		log.Fatalf("exec component requires non-nil channels")
	}
	if executeRequest == nil {
		log.Fatalf("exec component requires ExecuteRequest")
	}
	initialKV := map[string]string{"1": "111"}
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
		VerifierCh:            verifierCh,
		ShimCh:                shimCh,
		ExecuteRequest:        executeRequest,
		stableState:           stable,
		workingState:          working,
		pendingResponses:      make(map[int]pendingResponse),
		u:                     1,
		r:                     0,
		view:                  1,
		verifyResponseMsgs:    make(map[string]map[string]any),
		verifyResponseBySeq:   make(map[int]map[string]struct{}),
		checkpoints:           make(map[int]rollbackCheckpoint),
		verifyResponseTimeout: 2 * time.Second,
		verifyResponseTimers:  make(map[int]*time.Timer),
		batchBuffer:           common.NewOOOBuffer[map[string]any](),
		verifyBuffer:          common.NewOOOBuffer[map[string]any](),
		nextBatchSeq:          1,
		nextVerifySeq:         1,
		recoveringTransfers:   make(map[int]struct{}),
		workerCount:           4,
	}
	exec.verifyResponseQuorum = common.NewQuorumHelper(exec.r + 1)
	exec.storeCheckpoint(0, stable.PrevHash, stable.Merkle, stable.MerkleRoot)
	exec.scheduler = newExecScheduler()
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
	e.workingState.Merkle.Set(key, value)
	e.workingState.KVStore = e.workingState.Merkle.SnapshotMap()
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
			e.workingState.Merkle.Set(key, e.batchCtx.pendingNew[key])
		}
	}
	e.workingState.KVStore = e.workingState.Merkle.SnapshotMap()
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

func (e *Exec) ConsumeNestedResponse(requestID any) (map[string]any, bool) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return nil, false
	}
	return e.scheduler.popNestedResponse(canonicalID)
}

func (e *Exec) HandleBatchMessage(payload map[string]any) map[string]any {
	log.Printf("Handler called on %s with payload: %v", e.Name, payload)
	seqNum := common.GetInt(payload, "seq_num")
	e.batchBuffer.Add(seqNum, payload)
	e.processMu.Lock()
	defer e.processMu.Unlock()
	for {
		progressed := false
		if e.flushNextBatch() {
			progressed = true
		}
		if e.flushNextVerify() {
			progressed = true
		}
		if !progressed {
			break
		}
	}
	return map[string]any{"status": "buffered", "seq_num": seqNum}
}

func (e *Exec) HandleVerifyResponseMessage(payload map[string]any) map[string]any {
	log.Printf("Handler called on %s with payload: %v", e.Name, payload)
	seqNum := common.GetInt(payload, "seq_num")
	e.verifyBuffer.Add(seqNum, payload)
	e.processMu.Lock()
	defer e.processMu.Unlock()
	for {
		if !e.flushNextVerify() {
			break
		}
	}
	return map[string]any{"status": "buffered", "seq_num": seqNum}
}

func (e *Exec) HandleStateTransferRequestMessage(payload map[string]any) map[string]any {
	return e.handleStateTransferRequest(payload)
}
