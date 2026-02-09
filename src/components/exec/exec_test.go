package exec

import (
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"aegean/common"
)

type testServer struct {
	server   *http.Server
	listener net.Listener
	received chan map[string]any
	handler  func(map[string]any) map[string]any
}

func startTestServer(t *testing.T, handler func(map[string]any) map[string]any) *testServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:8000")
	if err != nil {
		t.Fatalf("failed to listen on 127.0.0.1:8000: %v", err)
	}

	ts := &testServer{
		server:   &http.Server{},
		listener: listener,
		received: make(chan map[string]any, 64),
		handler:  handler,
	}

	ts.server.Handler = common.MakeHandler(func(req map[string]any) map[string]any {
		ts.received <- req
		if ts.handler != nil {
			return ts.handler(req)
		}
		return map[string]any{"status": "ok"}
	})

	go func() {
		_ = ts.server.Serve(listener)
	}()

	return ts
}

func (ts *testServer) close() {
	_ = ts.server.Close()
	_ = ts.listener.Close()
}

func expectMessage(t *testing.T, ch <-chan map[string]any, wantType string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(750 * time.Millisecond)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for message type %q", wantType)
		}
		select {
		case msg := <-ch:
			if wantType == "" {
				return msg
			}
			if got, _ := msg["type"].(string); got == wantType {
				return msg
			}
		case <-time.After(remaining):
			t.Fatalf("timed out waiting for message type %q", wantType)
		}
	}
}

func makeSpinRequest(id string, writeKey string, writeValue string, readKey string) map[string]any {
	return map[string]any{
		"request_id": id,
		"op":         "spin_write_read",
		"op_payload": map[string]any{
			"spin_time":   0,
			"write_key":   writeKey,
			"write_value": writeValue,
			"read_key":    readKey,
		},
	}
}

func makeParallelBatches(requests ...map[string]any) [][]map[string]any {
	return [][]map[string]any{requests}
}

func newTestExec(name string, verifiers []string, peers []string) (*Exec, chan map[string]any, chan map[string]any) {
	verifierCh := make(chan map[string]any, 64)
	shimCh := make(chan map[string]any, 64)
	exec := NewExec(name, verifiers, peers, verifierCh, shimCh, testExecuteRequest)
	return exec, verifierCh, shimCh
}

func testExecuteRequest(e *Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	requestID := request["request_id"]
	op, _ := request["op"].(string)
	opPayload, _ := request["op_payload"].(map[string]any)

	response := map[string]any{"request_id": requestID}

	switch op {
	case "spin_write_read":
		spinTime := getFloatTest(opPayload, "spin_time")
		writeKey := getStringTest(opPayload, "write_key")
		writeValue := getStringTest(opPayload, "write_value")
		readKey := getStringTest(opPayload, "read_key")

		if spinTime > 0 {
			time.Sleep(time.Duration(spinTime * float64(time.Second)))
		}

		e.WriteKV(writeKey, writeValue)
		response["read_value"] = e.ReadKV(readKey)
		response["status"] = "ok"
	default:
		response["status"] = "error"
		response["error"] = "Unknown op: " + op
	}

	_ = ndSeed
	_ = ndTimestamp
	return response
}

func getStringTest(m map[string]any, key string) string {
	if value, ok := m[key]; ok {
		if s, ok := value.(string); ok {
			return s
		}
	}
	return ""
}

func getFloatTest(m map[string]any, key string) float64 {
	if value, ok := m[key]; ok {
		switch v := value.(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		}
	}
	return 0
}

// Executes a batch, records a snapshot, and emits a verify message with the computed token
func TestExecHandleBatchSendsVerifyAndTracksPending(t *testing.T) {
	exec, verifierCh, _ := newTestExec("exec1", []string{"exec1"}, nil)

	payload := map[string]any{
		"type":    "batch",
		"seq_num": 1,
		"parallel_batches": makeParallelBatches(
			makeSpinRequest("r1", "k1", "v1", "1"),
			makeSpinRequest("r2", "k2", "v2", "k1"),
		),
		"nd_seed":      int64(7),
		"nd_timestamp": float64(123.45),
	}

	resp := exec.handleBatch(payload)
	if resp["status"] != "executed" {
		t.Fatalf("expected status executed, got %v", resp["status"])
	}
	exec.flushNextVerify()

	pending, ok := exec.pendingResponses[1]
	if !ok {
		t.Fatalf("expected pending response for seq_num 1")
	}
	if len(pending.outputs) != 2 {
		t.Fatalf("expected 2 outputs, got %d", len(pending.outputs))
	}

	expectedToken := exec.computeStateHash(pending.state, pending.outputs, exec.stableState.PrevHash, 1)
	if pending.token != expectedToken {
		t.Fatalf("expected pending token %s, got %s", expectedToken, pending.token)
	}
	exec.workingState.KVStore["mutate"] = "later"
	if _, ok := pending.state["mutate"]; ok {
		t.Fatalf("expected pending state to be a snapshot, but it was mutated")
	}

	verifyMsg := expectMessage(t, verifierCh, "verify")
	if verifyMsg["seq_num"] != float64(1) && verifyMsg["seq_num"] != 1 {
		t.Fatalf("expected verify seq_num 1, got %v", verifyMsg["seq_num"])
	}
	if verifyMsg["token"] != expectedToken {
		t.Fatalf("expected verify token %s, got %v", expectedToken, verifyMsg["token"])
	}
	if verifyMsg["exec_id"] != "exec1" {
		t.Fatalf("expected exec_id exec1, got %v", verifyMsg["exec_id"])
	}
}

// Commit decision stabilizes state/prev-hash and releases responses to the shim
func TestExecVerifyCommitStabilizesAndResponds(t *testing.T) {
	exec, _, shimCh := newTestExec("exec1", []string{"exec1"}, nil)

	payload := map[string]any{
		"type":    "batch",
		"seq_num": 2,
		"parallel_batches": makeParallelBatches(
			makeSpinRequest("r1", "k1", "v1", "1"),
			makeSpinRequest("r2", "k2", "v2", "k1"),
		),
	}
	exec.handleBatch(payload)

	pending := exec.pendingResponses[2]
	commitResp := exec.handleVerifyResponse(map[string]any{
		"type":     "verify_response",
		"seq_num":  2,
		"decision": "commit",
		"token":    pending.token,
	})
	if commitResp["status"] != "processed" {
		t.Fatalf("expected processed status, got %v", commitResp["status"])
	}

	if exec.stableState.SeqNum != 2 {
		t.Fatalf("expected stableSeqNum 2, got %d", exec.stableState.SeqNum)
	}
	if exec.stableState.PrevHash != pending.token {
		t.Fatalf("expected prevHash to be committed token")
	}
	if exec.forceSequential {
		t.Fatalf("expected forceSequential false after commit")
	}

	if _, ok := exec.pendingResponses[2]; ok {
		t.Fatalf("expected pendingResponses to be cleared after commit")
	}

	// Expect two response messages sent to shim
	resp1 := expectMessage(t, shimCh, "response")
	resp2 := expectMessage(t, shimCh, "response")
	_ = resp1
	_ = resp2
}

// Token mismatch triggers state transfer and applies a newer stable state
func TestExecVerifyMismatchTriggersStateTransfer(t *testing.T) {
	transferredState := map[string]any{"a": "10", "b": "20"}
	ts := startTestServer(t, func(req map[string]any) map[string]any {
		if req["type"] == "state_transfer_request" {
			return map[string]any{
				"status":         "ok",
				"state":          transferredState,
				"stable_seq_num": 5,
				"prev_hash":      "hash-after-transfer",
			}
		}
		return map[string]any{"status": "ok"}
	})
	defer ts.close()

	exec, _, _ := newTestExec("exec1", []string{"exec1"}, []string{"127.0.0.1"})
	exec.stableState.SeqNum = 1
	exec.stableState.KVStore = map[string]string{"x": "1"}
	exec.workingState.KVStore = common.CopyStringMap(exec.stableState.KVStore)

	payload := map[string]any{
		"type":    "batch",
		"seq_num": 2,
		"parallel_batches": makeParallelBatches(
			makeSpinRequest("r1", "k1", "v1", "1"),
		),
	}
	exec.handleBatch(payload)

	pending := exec.pendingResponses[2]
	exec.handleVerifyResponse(map[string]any{
		"type":     "verify_response",
		"seq_num":  2,
		"decision": "commit",
		"token":    pending.token + "-mismatch",
	})

	expectMessage(t, ts.received, "state_transfer_request")

	if exec.stableState.SeqNum != 5 {
		t.Fatalf("expected stableSeqNum updated to 5, got %d", exec.stableState.SeqNum)
	}
	if exec.stableState.PrevHash != "hash-after-transfer" {
		t.Fatalf("expected prevHash updated after transfer")
	}
	if exec.forceSequential {
		t.Fatalf("expected forceSequential false after successful state transfer")
	}
	if exec.workingState.KVStore["a"] != "10" || exec.workingState.KVStore["b"] != "20" {
		t.Fatalf("expected kvStore to match transferred state, got %v", exec.workingState.KVStore)
	}
}

// Out-of-order batches are buffered until the missing seq arrives
func TestExecBuffersOutOfOrderBatches(t *testing.T) {
	exec, _, _ := newTestExec("exec1", []string{"exec1"}, nil)

	batch2 := map[string]any{
		"type":    "batch",
		"seq_num": 2,
		"parallel_batches": makeParallelBatches(
			makeSpinRequest("r2", "k2", "v2", "1"),
		),
	}
	resp2 := exec.HandleBatchMessage(batch2)
	if resp2["status"] != "buffered" {
		t.Fatalf("expected buffered status for seq 2, got %v", resp2["status"])
	}
	if _, ok := exec.pendingResponses[2]; ok {
		t.Fatalf("expected no pending response for seq 2 before seq 1 arrives")
	}

	batch1 := map[string]any{
		"type":    "batch",
		"seq_num": 1,
		"parallel_batches": makeParallelBatches(
			makeSpinRequest("r1", "k1", "v1", "1"),
		),
	}
	exec.HandleBatchMessage(batch1)

	if _, ok := exec.pendingResponses[1]; !ok {
		t.Fatalf("expected pending response for seq 1 after flush")
	}
	if _, ok := exec.pendingResponses[2]; !ok {
		t.Fatalf("expected pending response for seq 2 after flush")
	}

}

// Verify responses arriving before their batches are buffered and flushed later
func TestExecBuffersVerifyBeforeBatch(t *testing.T) {
	exec, _, _ := newTestExec("exec1", []string{"exec1"}, nil)

	verifyResp := map[string]any{
		"type":     "verify_response",
		"seq_num":  1,
		"decision": "commit",
		"token":    "mismatch-token",
	}
	resp := exec.HandleVerifyResponseMessage(verifyResp)
	if resp["status"] != "buffered" {
		t.Fatalf("expected buffered status for verify response, got %v", resp["status"])
	}

	batch1 := map[string]any{
		"type":    "batch",
		"seq_num": 1,
		"parallel_batches": makeParallelBatches(
			makeSpinRequest("r1", "k1", "v1", "1"),
		),
	}
	exec.HandleBatchMessage(batch1)

	if _, ok := exec.pendingResponses[1]; ok {
		t.Fatalf("expected pending response cleared after buffered verify flush")
	}
}

// Token mismatch falls back to rollback when state transfer fails
func TestExecVerifyMismatchFallbackRollback(t *testing.T) {
	ts := startTestServer(t, func(req map[string]any) map[string]any {
		if req["type"] == "state_transfer_request" {
			return map[string]any{"status": "error"}
		}
		return map[string]any{"status": "ok"}
	})
	defer ts.close()

	exec, _, _ := newTestExec("exec1", []string{"exec1"}, []string{"127.0.0.1"})
	exec.stableState.KVStore = map[string]string{"stable": "yes"}
	exec.workingState.KVStore = map[string]string{"dirty": "no"}

	payload := map[string]any{
		"type":    "batch",
		"seq_num": 3,
		"parallel_batches": makeParallelBatches(
			makeSpinRequest("r1", "k1", "v1", "1"),
		),
	}
	exec.handleBatch(payload)

	pending := exec.pendingResponses[3]
	exec.handleVerifyResponse(map[string]any{
		"type":     "verify_response",
		"seq_num":  3,
		"decision": "commit",
		"token":    pending.token + "-mismatch",
	})

	expectMessage(t, ts.received, "state_transfer_request")

	if exec.workingState.KVStore["stable"] != "yes" || len(exec.workingState.KVStore) != 1 {
		t.Fatalf("expected rollback to stable state, got %v", exec.workingState.KVStore)
	}
	if !exec.forceSequential {
		t.Fatalf("expected forceSequential true after rollback")
	}
}

// Rollback decision reverts to stable state and forces sequential execution
func TestExecRollbackDecisionForcesSequential(t *testing.T) {
	exec, _, _ := newTestExec("exec1", nil, nil)
	exec.stableState.KVStore = map[string]string{"stable": "yes"}
	exec.workingState.KVStore = map[string]string{"dirty": "no"}
	exec.pendingResponses[4] = pendingResponse{
		outputs: []map[string]any{{"request_id": "r1", "status": "ok"}},
		state:   map[string]string{"dirty": "no"},
		token:   "t1",
	}

	exec.handleVerifyResponse(map[string]any{
		"type":     "verify_response",
		"seq_num":  4,
		"decision": "rollback",
	})

	if exec.workingState.KVStore["stable"] != "yes" || len(exec.workingState.KVStore) != 1 {
		t.Fatalf("expected rollback to stable state, got %v", exec.workingState.KVStore)
	}
	if !exec.forceSequential {
		t.Fatalf("expected forceSequential true after rollback")
	}
	if _, ok := exec.pendingResponses[4]; ok {
		t.Fatalf("expected pendingResponses to be cleared after rollback")
	}
}

// Hashing is deterministic across different map insertion orders
func TestComputeStateHashDeterministicOrdering(t *testing.T) {
	exec, _, _ := newTestExec("exec1", nil, nil)

	stateA := map[string]string{}
	stateA["b"] = "2"
	stateA["a"] = "1"

	stateB := map[string]string{}
	stateB["a"] = "1"
	stateB["b"] = "2"

	outputsA := []map[string]any{{"z": 1, "a": 2}}
	outputsB := []map[string]any{{"a": 2, "z": 1}}

	hashA := exec.computeStateHash(stateA, outputsA, "prev", 1)
	hashB := exec.computeStateHash(stateB, outputsB, "prev", 1)

	if hashA != hashB {
		t.Fatalf("expected deterministic hash, got %s and %s", hashA, hashB)
	}
}

func TestExecBlockedRequestResumesAfterNestedResponse(t *testing.T) {
	verifierCh := make(chan map[string]any, 8)
	shimCh := make(chan map[string]any, 8)
	var exec *Exec
	exec = NewExec("exec1", []string{"exec1"}, nil, verifierCh, shimCh,
		func(_ *Exec, request map[string]any, _ int64, _ float64) map[string]any {
			if nested, ok := exec.ConsumeNestedResponse(request["request_id"]); ok && nested != nil {
				return map[string]any{
					"request_id": request["request_id"],
					"status":     "ok",
					"nested":     nested["status"],
				}
			}
			return map[string]any{
				"request_id": request["request_id"],
				"status":     "blocked_for_nested_response",
			}
		},
	)

	done := make(chan map[string]any, 1)
	go func() {
		done <- exec.handleBatch(map[string]any{
			"type":    "batch",
			"seq_num": 1,
			"parallel_batches": makeParallelBatches(
				map[string]any{"request_id": "r1", "op": "block"},
			),
		})
	}()

	time.Sleep(25 * time.Millisecond)
	if !exec.BufferNestedResponse(map[string]any{"request_id": "r1", "status": "ready"}) {
		t.Fatalf("expected nested response to be accepted for in-flight request")
	}

	select {
	case resp := <-done:
		if resp["status"] != "executed" {
			t.Fatalf("expected handleBatch to finish execution, got %v", resp["status"])
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for blocked request to resume")
	}

	pending, ok := exec.pendingResponses[1]
	if !ok {
		t.Fatalf("expected pending response for seq 1")
	}
	if len(pending.outputs) != 1 {
		t.Fatalf("expected one output, got %d", len(pending.outputs))
	}
	if pending.outputs[0]["status"] != "ok" {
		t.Fatalf("expected resumed output status ok, got %v", pending.outputs[0]["status"])
	}
}

func TestExecParallelBatchesYieldBlockedToNext(t *testing.T) {
	verifierCh := make(chan map[string]any, 8)
	shimCh := make(chan map[string]any, 8)
	var exec *Exec
	exec = NewExec("exec1", []string{"exec1"}, nil, verifierCh, shimCh,
		func(_ *Exec, request map[string]any, _ int64, _ float64) map[string]any {
			op, _ := request["op"].(string)
			requestID := request["request_id"]
			switch op {
			case "block":
				if nested, ok := exec.ConsumeNestedResponse(requestID); ok && nested != nil {
					return map[string]any{"request_id": requestID, "status": "ok"}
				}
				return map[string]any{"request_id": requestID, "status": "blocked_for_nested_response"}
			case "mark":
				exec.WriteKV("next_batch_progress", "yes")
				return map[string]any{"request_id": requestID, "status": "ok"}
			default:
				return map[string]any{"request_id": requestID, "status": "error"}
			}
		},
	)
	exec.scheduler.parallelWindowK = 2

	done := make(chan map[string]any, 1)
	go func() {
		done <- exec.handleBatch(map[string]any{
			"type":    "batch",
			"seq_num": 1,
			"parallel_batches": [][]map[string]any{
				{map[string]any{"request_id": "r1", "op": "block"}},
				{map[string]any{"request_id": "r2", "op": "mark"}},
			},
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := exec.ReadKV("next_batch_progress"); got == "yes" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected scheduler to yield to next batch while first is blocked")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !exec.BufferNestedResponse(map[string]any{"request_id": "r1", "status": "ready"}) {
		t.Fatalf("expected nested response to be accepted for in-flight request")
	}

	select {
	case resp := <-done:
		if resp["status"] != "executed" {
			t.Fatalf("expected status executed, got %v", resp["status"])
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for batch completion")
	}
}

func TestExecParallelBatchWindowGatesByStableSeq(t *testing.T) {
	verifierCh := make(chan map[string]any, 8)
	shimCh := make(chan map[string]any, 8)
	var exec *Exec
	exec = NewExec("exec1", []string{"exec1"}, nil, verifierCh, shimCh,
		func(_ *Exec, request map[string]any, _ int64, _ float64) map[string]any {
			op, _ := request["op"].(string)
			requestID := request["request_id"]
			switch op {
			case "block":
				if nested, ok := exec.ConsumeNestedResponse(requestID); ok && nested != nil {
					return map[string]any{"request_id": requestID, "status": "ok"}
				}
				return map[string]any{"request_id": requestID, "status": "blocked_for_nested_response"}
			case "mark":
				key, _ := request["key"].(string)
				exec.WriteKV(key, "done")
				return map[string]any{"request_id": requestID, "status": "ok"}
			default:
				return map[string]any{"request_id": requestID, "status": "error"}
			}
		},
	)
	exec.scheduler.parallelWindowK = 2

	done := make(chan map[string]any, 1)
	go func() {
		done <- exec.handleBatch(map[string]any{
			"type":    "batch",
			"seq_num": 1,
			"parallel_batches": [][]map[string]any{
				{map[string]any{"request_id": "r1", "op": "block"}},
				{map[string]any{"request_id": "r2", "op": "mark", "key": "b1"}},
				{map[string]any{"request_id": "r3", "op": "mark", "key": "b2"}},
			},
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := exec.ReadKV("b1"); got == "done" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected batch n+1 inside window to run while n is blocked")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := exec.ReadKV("b2"); got != "" {
		t.Fatalf("expected batch outside v..v+k window to wait, got %q", got)
	}

	if !exec.BufferNestedResponse(map[string]any{"request_id": "r1", "status": "ready"}) {
		t.Fatalf("expected nested response to be accepted for in-flight request")
	}

	select {
	case resp := <-done:
		if resp["status"] != "executed" {
			t.Fatalf("expected status executed, got %v", resp["status"])
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for batch completion")
	}

	if got := exec.ReadKV("b2"); got != "done" {
		t.Fatalf("expected wrapped window to eventually execute later batch, got %q", got)
	}
}

func TestExecParallelBatchSchedulingDeterministic(t *testing.T) {
	verifierCh := make(chan map[string]any, 8)
	shimCh := make(chan map[string]any, 8)
	var mu sync.Mutex
	trace := make([]string, 0, 8)
	var exec *Exec
	exec = NewExec("exec1", []string{"exec1"}, nil, verifierCh, shimCh,
		func(_ *Exec, request map[string]any, _ int64, _ float64) map[string]any {
			requestID, _ := request["request_id"].(string)
			mu.Lock()
			trace = append(trace, requestID)
			mu.Unlock()
			if nested, ok := exec.ConsumeNestedResponse(requestID); ok && nested != nil {
				return map[string]any{"request_id": requestID, "status": "ok"}
			}
			return map[string]any{"request_id": requestID, "status": "blocked_for_nested_response"}
		},
	)
	exec.workerCount = 1
	exec.scheduler.parallelWindowK = 2

	done := make(chan map[string]any, 1)
	go func() {
		done <- exec.handleBatch(map[string]any{
			"type":    "batch",
			"seq_num": 1,
			"parallel_batches": [][]map[string]any{
				{map[string]any{"request_id": "r1", "op": "block"}},
				{map[string]any{"request_id": "r2", "op": "block"}},
			},
		})
	}()

	time.Sleep(40 * time.Millisecond)
	if !exec.BufferNestedResponse(map[string]any{"request_id": "r1", "status": "ready"}) {
		t.Fatalf("expected nested response for r1 to be accepted")
	}
	if !exec.BufferNestedResponse(map[string]any{"request_id": "r2", "status": "ready"}) {
		t.Fatalf("expected nested response for r2 to be accepted")
	}

	select {
	case resp := <-done:
		if resp["status"] != "executed" {
			t.Fatalf("expected status executed, got %v", resp["status"])
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for deterministic scheduling run")
	}

	mu.Lock()
	got := append([]string(nil), trace...)
	mu.Unlock()
	want := []string{"r1", "r2", "r2", "r1"}
	if len(got) != len(want) {
		t.Fatalf("expected deterministic trace length %d, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected deterministic trace %v, got %v", want, got)
		}
	}
}

func TestExecParallelBatchConflictsRemainDeterministicAcrossExecs(t *testing.T) {
	execA, verifierA, _ := newTestExec("execA", []string{"execA"}, nil)
	execB, verifierB, _ := newTestExec("execB", []string{"execB"}, nil)

	execA.workerCount = 4
	execB.workerCount = 4
	execA.scheduler.parallelWindowK = 4
	execB.scheduler.parallelWindowK = 4

	parallelBatches := make([][]map[string]any, 0, 10)
	for i := 1; i <= 10; i++ {
		req := map[string]any{
			"request_id": strconv.Itoa(i),
			"op":         "spin_write_read",
			"op_payload": map[string]any{
				"spin_time":   0.1,
				"write_key":   "1",
				"write_value": "value_" + strconv.Itoa(i),
				"read_key":    "1",
			},
		}
		parallelBatches = append(parallelBatches, []map[string]any{req})
	}

	payload := map[string]any{
		"type":             "batch",
		"seq_num":          1,
		"parallel_batches": parallelBatches,
		"nd_seed":          int64(11),
		"nd_timestamp":     float64(123.45),
	}

	respA := execA.handleBatch(payload)
	respB := execB.handleBatch(payload)
	if respA["status"] != "executed" || respB["status"] != "executed" {
		t.Fatalf("expected both execs to execute batch, got %v and %v", respA["status"], respB["status"])
	}

	execA.flushNextVerify()
	execB.flushNextVerify()
	_ = expectMessage(t, verifierA, "verify")
	_ = expectMessage(t, verifierB, "verify")

	pendingA, ok := execA.pendingResponses[1]
	if !ok {
		t.Fatalf("expected pending response on execA")
	}
	pendingB, ok := execB.pendingResponses[1]
	if !ok {
		t.Fatalf("expected pending response on execB")
	}
	if pendingA.token != pendingB.token {
		t.Fatalf("expected deterministic tokens across execs, got %s vs %s", pendingA.token, pendingB.token)
	}
}
