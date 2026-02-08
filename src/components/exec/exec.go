package exec

import (
	"log"
	"strings"
	"sync"

	"aegean/common"
)

type pendingResponse struct {
	outputs []map[string]any
	state   map[string]string
	token   string
	// verifySent indicates whether a verify message has been sent for this seq
	verifySent bool
}

// ExecuteRequestFunc handles a single request for an exec node
// ExecuteResponseFunc handles a response message for an exec node
// ResponseSink forwards responses (e.g. to a shim).
type ExecuteRequestFunc func(e *Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any
type ExecuteResponseFunc func(e *Exec, payload map[string]any) map[string]any

type Exec struct {
	Name      string
	Verifiers []string
	Peers     []string
	// Local component channels
	VerifierCh chan<- map[string]any
	ShimCh     chan<- map[string]any
	mu         sync.Mutex
	// State management for rollback
	stableState  State
	workingState State
	// Pending responses (held until commit)
	pendingResponses map[int]pendingResponse
	// Sequential execution flag (set after rollback)
	forceSequential bool
	// Out-of-order buffers
	batchBuffer   *common.OOOBuffer[map[string]any]
	verifyBuffer  *common.OOOBuffer[map[string]any]
	nextBatchSeq  int
	nextVerifySeq int
	// Request execution hook
	ExecuteRequest ExecuteRequestFunc
	// Response handling hook
	HandleResponse ExecuteResponseFunc
}

// TODO: request pipelining, parallel pipelining
// TODO: implement locking
func NewExec(name string, verifiers []string, peers []string, verifierCh chan<- map[string]any, shimCh chan<- map[string]any, executeRequest ExecuteRequestFunc, handleResponse ExecuteResponseFunc) *Exec {
	if verifierCh == nil || shimCh == nil {
		log.Fatalf("exec component requires non-nil channels")
	}
	if executeRequest == nil {
		log.Fatalf("exec component requires ExecuteRequest")
	}
	if handleResponse == nil {
		log.Fatalf("exec component requires HandleResponse")
	}
	initialKV := map[string]string{"1": "111"}
	stable := State{
		KVStore:  common.CopyStringMap(initialKV),
		SeqNum:   0,
		PrevHash: strings.Repeat("0", 64),
		Verified: true,
	}
	working := State{
		KVStore:  initialKV,
		SeqNum:   0,
		PrevHash: stable.PrevHash,
		Verified: false,
	}
	exec := &Exec{
		Name:             name,
		Verifiers:        verifiers,
		Peers:            peers,
		VerifierCh:       verifierCh,
		ShimCh:           shimCh,
		ExecuteRequest:   executeRequest,
		HandleResponse:   handleResponse,
		stableState:      stable,
		workingState:     working,
		pendingResponses: make(map[int]pendingResponse),
		batchBuffer:      common.NewOOOBuffer[map[string]any](),
		verifyBuffer:     common.NewOOOBuffer[map[string]any](),
		nextBatchSeq:     1,
		nextVerifySeq:    1,
	}
	return exec
}

func (e *Exec) ReadKV(key string) string {
	return e.workingState.KVStore[key]
}

func (e *Exec) WriteKV(key, value string) {
	e.workingState.KVStore[key] = value
}

func (e *Exec) HandleBatchMessage(payload map[string]any) map[string]any {
	log.Printf("Handler called on %s with payload: %v", e.Name, payload)
	seqNum := common.GetInt(payload, "seq_num")
	e.batchBuffer.Add(seqNum, payload)
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
