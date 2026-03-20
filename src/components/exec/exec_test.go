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

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s", message)
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
	exec := NewExec(name, verifiers, peers, verifierCh, shimCh, 1, testExecuteRequest, nil, requiredExecRunConfig())
	return exec, verifierCh, shimCh
}

func requiredExecRunConfig() map[string]any {
	return map[string]any{
		"worker_count":      4,
		"parallel_window_k": 4,
	}
}

func TestExecRunConfigOverrides(t *testing.T) {
	verifierCh := make(chan map[string]any, 64)
	shimCh := make(chan map[string]any, 64)
	exec := NewExec("exec1", []string{"exec1"}, nil, verifierCh, shimCh, 1, testExecuteRequest, nil, map[string]any{
		"worker_count":      3,
		"parallel_window_k": 6,
	})

	if exec.workerCount != 3 {
		t.Fatalf("expected workerCount 3, got %d", exec.workerCount)
	}
	if exec.scheduler.parallelWindowK != 6 {
		t.Fatalf("expected parallelWindowK 6, got %d", exec.scheduler.parallelWindowK)
	}
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

	pending, ok := exec.pendingExecResults[1]
	if !ok {
		t.Fatalf("expected pending response for seq_num 1")
	}
	if len(pending.outputs) != 2 {
		t.Fatalf("expected 2 outputs, got %d", len(pending.outputs))
	}

	expectedToken := exec.computeStateHash(pending.merkleRoot, pending.outputs, exec.stableState.PrevHash, 1)
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

	pending := exec.pendingExecResults[2]
	pending.token = exec.computeStateHash(pending.merkleRoot, pending.outputs, exec.stableState.PrevHash, 2)
	exec.pendingExecResults[2] = pending
	commitResp := exec.handleVerifyResponse(map[string]any{
		"type":             "verify_response",
		"view":             1,
		"seq_num":          2,
		"token":            pending.token,
		"force_sequential": false,
		"verifier_id":      "ver1",
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
	if _, ok := exec.checkpoints[0]; ok {
		t.Fatalf("expected checkpoint 0 to be garbage-collected after commit")
	}
	if _, ok := exec.checkpoints[2]; !ok {
		t.Fatalf("expected checkpoint 2 to be retained after commit")
	}
	if len(exec.checkpoints) != 1 {
		t.Fatalf("expected only highest stable checkpoint to remain, got %d", len(exec.checkpoints))
	}

	if _, ok := exec.pendingExecResults[2]; ok {
		t.Fatalf("expected pendingExecResults to be cleared after commit")
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
	transferredMerkle := NewMerkleTreeFromMap(map[string]string{"a": "10", "b": "20"})
	ts := startTestServer(t, func(req map[string]any) map[string]any {
		if req["type"] == "state_transfer_request" {
			return map[string]any{
				"status":         "ok",
				"mode":           "delta",
				"updates":        transferredState,
				"deletes":        []string{"x"},
				"stable_seq_num": 5,
				"prev_hash":      "hash-after-transfer",
				"state_root":     transferredMerkle.Root(),
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

	pending := exec.pendingExecResults[2]
	exec.handleVerifyResponse(map[string]any{
		"type":             "verify_response",
		"view":             1,
		"seq_num":          2,
		"token":            pending.token + "-mismatch",
		"force_sequential": false,
		"verifier_id":      "ver1",
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
	if _, ok := exec.checkpoints[0]; ok {
		t.Fatalf("expected checkpoint 0 to be garbage-collected after state transfer")
	}
	if _, ok := exec.checkpoints[5]; !ok {
		t.Fatalf("expected checkpoint 5 to be retained after state transfer")
	}
	if len(exec.checkpoints) != 1 {
		t.Fatalf("expected only highest stable checkpoint to remain, got %d", len(exec.checkpoints))
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
	if _, ok := exec.pendingExecResults[2]; ok {
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
	waitForCondition(t, 500*time.Millisecond, func() bool {
		_, ok1 := exec.pendingExecResults[1]
		_, ok2 := exec.pendingExecResults[2]
		return ok1 && ok2
	}, "expected pending responses for seq 1 and 2 after flush")

}

// Committing an earlier sequence should not rewind working state when higher
// speculative sequences are already pending; otherwise later batches execute on stale base.
func TestExecCommitPreservesSpeculativeWorkingTip(t *testing.T) {
	exec, _, _ := newTestExec("exec1", []string{"exec1"}, nil)

	batch1 := map[string]any{
		"type":    "batch",
		"seq_num": 1,
		"parallel_batches": makeParallelBatches(
			makeSpinRequest("1", "1", "value_1", "1"),
		),
	}
	batch2 := map[string]any{
		"type":    "batch",
		"seq_num": 2,
		"parallel_batches": makeParallelBatches(
			makeSpinRequest("2", "2", "value_2", "2"),
			makeSpinRequest("3", "3", "value_3", "3"),
			makeSpinRequest("4", "4", "value_4", "4"),
			makeSpinRequest("5", "5", "value_5", "5"),
			makeSpinRequest("6", "6", "value_6", "6"),
			makeSpinRequest("7", "7", "value_7", "7"),
			makeSpinRequest("8", "8", "value_8", "8"),
			makeSpinRequest("9", "9", "value_9", "9"),
			makeSpinRequest("10", "10", "value_10", "10"),
			makeSpinRequest("11", "11", "value_11", "11"),
		),
	}
	batch3 := map[string]any{
		"type":    "batch",
		"seq_num": 3,
		"parallel_batches": makeParallelBatches(
			makeSpinRequest("12", "12", "value_12", "12"),
			makeSpinRequest("13", "13", "value_13", "13"),
			makeSpinRequest("14", "14", "value_14", "14"),
			makeSpinRequest("15", "15", "value_15", "15"),
			makeSpinRequest("16", "16", "value_16", "16"),
			makeSpinRequest("17", "17", "value_17", "17"),
			makeSpinRequest("18", "18", "value_18", "18"),
			makeSpinRequest("19", "19", "value_19", "19"),
			makeSpinRequest("20", "20", "value_20", "20"),
			makeSpinRequest("21", "21", "value_21", "21"),
		),
	}
	batch4 := map[string]any{
		"type":    "batch",
		"seq_num": 4,
		"parallel_batches": makeParallelBatches(
			makeSpinRequest("22", "22", "value_22", "22"),
			makeSpinRequest("23", "23", "value_23", "23"),
			makeSpinRequest("24", "24", "value_24", "24"),
			makeSpinRequest("25", "25", "value_25", "25"),
			makeSpinRequest("26", "26", "value_26", "26"),
			makeSpinRequest("27", "27", "value_27", "27"),
			makeSpinRequest("28", "28", "value_28", "28"),
			makeSpinRequest("29", "29", "value_29", "29"),
			makeSpinRequest("30", "30", "value_30", "30"),
			makeSpinRequest("31", "31", "value_31", "31"),
		),
	}

	exec.handleBatch(batch1)
	exec.handleBatch(batch2)
	exec.handleBatch(batch3)

	p1 := exec.pendingExecResults[1]
	p1.token = "token-1"
	exec.pendingExecResults[1] = p1
	exec.handleVerifyResponse(map[string]any{
		"type":             "verify_response",
		"view":             1,
		"seq_num":          1,
		"token":            "token-1",
		"force_sequential": false,
		"verifier_id":      "ver1",
	})

	exec.handleBatch(batch4)
	p4, ok := exec.pendingExecResults[4]
	if !ok {
		t.Fatalf("expected pending response for seq 4")
	}
	for _, key := range []string{"2", "11", "12", "21", "22", "31"} {
		if got := p4.state[key]; got == "" {
			t.Fatalf("expected seq 4 pending state to retain key %s after seq1 commit; state=%v", key, p4.state)
		}
	}
}

// While state transfer is in-flight (processMu held), incoming batches should not be
// enqueued early and then lost by transfer-time buffer clearing.
func TestExecHandleBatchDoesNotLoseBatchWhenProcessMuHeld(t *testing.T) {
	exec, _, _ := newTestExec("exec1", []string{"exec1"}, nil)
	batch := map[string]any{
		"type":    "batch",
		"seq_num": 1,
		"parallel_batches": makeParallelBatches(
			makeSpinRequest("r1", "k1", "v1", "1"),
		),
	}

	exec.processMu.Lock()
	done := make(chan map[string]any, 1)
	go func() {
		done <- exec.HandleBatchMessage(batch)
	}()

	// Simulate transfer logic clearing the batch buffer while processing is stalled.
	time.Sleep(10 * time.Millisecond)
	exec.batchBuffer.Clear()
	exec.processMu.Unlock()

	select {
	case resp := <-done:
		if resp["status"] != "buffered" {
			t.Fatalf("expected buffered status, got %v", resp["status"])
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for HandleBatchMessage")
	}

	waitForCondition(t, 500*time.Millisecond, func() bool {
		_, ok := exec.pendingExecResults[1]
		return ok
	}, "expected seq 1 pending result to survive transfer-time clear window")
}

// Verify responses arriving before their batches are buffered and flushed later
func TestExecBuffersVerifyBeforeBatch(t *testing.T) {
	ts := startTestServer(t, func(req map[string]any) map[string]any {
		if req["type"] == "state_transfer_request" {
			return map[string]any{
				"status":         "ok",
				"mode":           "full",
				"state":          map[string]any{"stable": "yes"},
				"stable_seq_num": 3,
				"prev_hash":      "token-3",
				"state_root":     NewMerkleTreeFromMap(map[string]string{"stable": "yes"}).Root(),
			}
		}
		return map[string]any{"status": "ok"}
	})
	defer ts.close()

	exec, _, _ := newTestExec("exec1", []string{"exec1"}, []string{"127.0.0.1"})

	verifyResp := map[string]any{
		"type":             "verify_response",
		"view":             1,
		"seq_num":          1,
		"token":            "mismatch-token",
		"force_sequential": false,
		"verifier_id":      "ver1",
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

	expectMessage(t, ts.received, "state_transfer_request")

	if _, ok := exec.pendingExecResults[1]; ok {
		t.Fatalf("expected pending response cleared after buffered verify flush")
	}
}

// Token mismatch triggers state transfer and applies the transferred state.
func TestExecVerifyMismatchFallbackRollback(t *testing.T) {
	ts := startTestServer(t, func(req map[string]any) map[string]any {
		if req["type"] == "state_transfer_request" {
			return map[string]any{
				"status":         "ok",
				"mode":           "full",
				"state":          map[string]any{"stable": "yes"},
				"stable_seq_num": 5,
				"prev_hash":      "token-5",
				"state_root":     NewMerkleTreeFromMap(map[string]string{"stable": "yes"}).Root(),
			}
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

	pending := exec.pendingExecResults[3]
	exec.handleVerifyResponse(map[string]any{
		"type":             "verify_response",
		"view":             1,
		"seq_num":          3,
		"token":            pending.token + "-mismatch",
		"force_sequential": false,
		"verifier_id":      "ver1",
	})

	expectMessage(t, ts.received, "state_transfer_request")

	if exec.stableState.SeqNum != 5 {
		t.Fatalf("expected stable state to advance from transfer, got seq %d", exec.stableState.SeqNum)
	}
	if exec.workingState.KVStore["stable"] != "yes" || len(exec.workingState.KVStore) != 1 {
		t.Fatalf("expected transferred working state, got %v", exec.workingState.KVStore)
	}
}

// Rollback decision reverts to stable state and forces sequential execution
func TestExecRollbackDecisionForcesSequential(t *testing.T) {
	exec, _, _ := newTestExec("exec1", nil, nil)
	checkpointKV := map[string]string{"stable": "yes"}
	checkpointMerkle := NewMerkleTreeFromMap(checkpointKV)
	exec.storeCheckpoint(4, "t1", checkpointMerkle.SnapshotMap(), checkpointMerkle.Root())
	exec.pendingExecResults[4] = pendingExecResult{
		outputs: []map[string]any{{"request_id": "r1", "status": "ok"}},
		state:   map[string]string{"dirty": "no"},
		token:   "t1",
	}

	exec.handleVerifyResponse(map[string]any{
		"type":             "verify_response",
		"view":             2,
		"seq_num":          4,
		"token":            "t1",
		"force_sequential": true,
		"verifier_id":      "ver1",
	})

	if exec.stableState.SeqNum != 4 {
		t.Fatalf("expected rollback to checkpoint seq 4, got %d", exec.stableState.SeqNum)
	}
	if !exec.forceSequential {
		t.Fatalf("expected forceSequential true after rollback")
	}
	if _, ok := exec.pendingExecResults[4]; ok {
		t.Fatalf("expected pendingExecResults to be cleared after rollback")
	}
}

// View-change rollback at already stable seq should still force sequential and clear higher pending work.
func TestExecRollbackAtStableSeqAppliesForceSequential(t *testing.T) {
	exec, _, _ := newTestExec("exec1", nil, nil)
	stableKV := map[string]string{"stable": "yes"}
	stableMerkle := NewMerkleTreeFromMap(stableKV)
	exec.stableState = State{
		KVStore:    common.CopyStringMap(stableKV),
		Merkle:     stableMerkle.Clone(),
		MerkleRoot: stableMerkle.Root(),
		SeqNum:     2,
		PrevHash:   "stable-token",
		Verified:   true,
	}
	exec.workingState = State{
		KVStore:    common.CopyStringMap(stableKV),
		Merkle:     stableMerkle.Clone(),
		MerkleRoot: stableMerkle.Root(),
		SeqNum:     2,
		PrevHash:   "stable-token",
		Verified:   false,
	}
	exec.storeCheckpoint(2, "stable-token", stableMerkle.SnapshotMap(), stableMerkle.Root())
	exec.pendingExecResults[3] = pendingExecResult{
		outputs: []map[string]any{{"request_id": "r3", "status": "ok"}},
		state:   map[string]string{"dirty": "no"},
		token:   "t3",
	}

	resp := exec.handleVerifyResponse(map[string]any{
		"type":             "verify_response",
		"view":             2,
		"seq_num":          2,
		"token":            "stable-token",
		"force_sequential": true,
		"verifier_id":      "ver1",
	})

	if resp["decision"] != "rollback" {
		t.Fatalf("expected rollback decision, got %v", resp["decision"])
	}
	if !exec.forceSequential {
		t.Fatalf("expected forceSequential true after rollback at stable seq")
	}
	if exec.view != 2 {
		t.Fatalf("expected view to advance to 2, got %d", exec.view)
	}
	if _, ok := exec.pendingExecResults[3]; ok {
		t.Fatalf("expected higher pending work to be cleared after rollback")
	}
}

func TestExecHandleVerifyResponseMessageProcessesStableSeqRollbackImmediately(t *testing.T) {
	exec, _, _ := newTestExec("exec1", nil, nil)
	stableKV := map[string]string{"stable": "yes"}
	stableMerkle := NewMerkleTreeFromMap(stableKV)
	exec.stableState = State{
		KVStore:    common.CopyStringMap(stableKV),
		Merkle:     stableMerkle.Clone(),
		MerkleRoot: stableMerkle.Root(),
		SeqNum:     2,
		PrevHash:   "stable-token",
		Verified:   true,
	}
	exec.workingState = State{
		KVStore:    common.CopyStringMap(stableKV),
		Merkle:     stableMerkle.Clone(),
		MerkleRoot: stableMerkle.Root(),
		SeqNum:     2,
		PrevHash:   "stable-token",
		Verified:   false,
	}
	exec.storeCheckpoint(2, "stable-token", stableMerkle.SnapshotMap(), stableMerkle.Root())
	exec.nextVerifySeq = 3 // Simulate outstanding seq=3 path.
	exec.pendingExecResults[3] = pendingExecResult{
		outputs: []map[string]any{{"request_id": "r3", "status": "ok"}},
		state:   map[string]string{"dirty": "no"},
		token:   "t3",
	}

	resp := exec.HandleVerifyResponseMessage(map[string]any{
		"type":             "verify_response",
		"view":             2,
		"seq_num":          2,
		"token":            "stable-token",
		"force_sequential": true,
		"verifier_id":      "ver1",
	})

	if resp["status"] != "buffered" {
		t.Fatalf("expected buffered response status, got %v", resp["status"])
	}
	waitForCondition(t, 500*time.Millisecond, func() bool {
		if !exec.forceSequential {
			return false
		}
		if exec.view != 2 {
			return false
		}
		_, ok := exec.pendingExecResults[3]
		return !ok
	}, "expected immediate rollback effects (forceSequential/view/pending clear)")
}

func TestExecRollbackReplaysUncommittedBatch(t *testing.T) {
	verifierCh := make(chan map[string]any, 8)
	shimCh := make(chan map[string]any, 8)
	runCount := 0
	exec := NewExec("exec1", []string{"exec1"}, nil, verifierCh, shimCh, 1,
		func(_ *Exec, request map[string]any, _ int64, _ float64) map[string]any {
			runCount++
			return map[string]any{"request_id": request["request_id"], "status": "ok"}
		},
		nil,
		requiredExecRunConfig(),
	)
	stableKV := map[string]string{"stable": "yes"}
	stableMerkle := NewMerkleTreeFromMap(stableKV)
	exec.stableState = State{
		KVStore:    common.CopyStringMap(stableKV),
		Merkle:     stableMerkle.Clone(),
		MerkleRoot: stableMerkle.Root(),
		SeqNum:     2,
		PrevHash:   "stable-token",
		Verified:   true,
	}
	exec.workingState = State{
		KVStore:    common.CopyStringMap(stableKV),
		Merkle:     stableMerkle.Clone(),
		MerkleRoot: stableMerkle.Root(),
		SeqNum:     2,
		PrevHash:   "stable-token",
		Verified:   false,
	}
	exec.storeCheckpoint(2, "stable-token", stableMerkle.SnapshotMap(), stableMerkle.Root())
	exec.nextBatchSeq = 3
	exec.nextVerifySeq = 3
	exec.handleBatch(map[string]any{
		"type":    "batch",
		"seq_num": 3,
		"parallel_batches": [][]map[string]any{
			{map[string]any{"request_id": "r3", "op": "nop"}},
		},
	})
	if runCount != 1 {
		t.Fatalf("expected initial execution count 1, got %d", runCount)
	}

	resp := exec.HandleVerifyResponseMessage(map[string]any{
		"type":             "verify_response",
		"view":             2,
		"seq_num":          2,
		"token":            "stable-token",
		"force_sequential": true,
		"verifier_id":      "ver1",
	})
	if resp["status"] != "buffered" {
		t.Fatalf("expected buffered response status, got %v", resp["status"])
	}
	waitForCondition(t, 500*time.Millisecond, func() bool {
		if runCount != 2 {
			return false
		}
		_, ok := exec.pendingExecResults[3]
		return ok
	}, "expected uncommitted batch to be replayed once after rollback")
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

	rootA := NewMerkleTreeFromMap(stateA).Root()
	rootB := NewMerkleTreeFromMap(stateB).Root()
	hashA := exec.computeStateHash(rootA, outputsA, "prev", 1)
	hashB := exec.computeStateHash(rootB, outputsB, "prev", 1)

	if hashA != hashB {
		t.Fatalf("expected deterministic hash, got %s and %s", hashA, hashB)
	}
}

func TestDeferredNewKeyInsertionDeterministicAtBatchEnd(t *testing.T) {
	execA, _, _ := newTestExec("execA", nil, nil)
	execB, _, _ := newTestExec("execB", nil, nil)

	execA.beginBatchMerkleContext()
	execA.WriteKV("z", "26")
	execA.WriteKV("a", "1")
	if got := execA.ReadKV("a"); got != "1" {
		t.Fatalf("expected pending new key to be readable during batch, got %q", got)
	}
	if _, ok := execA.workingState.KVStore["a"]; ok {
		t.Fatalf("expected new key to be deferred before finalize")
	}
	execA.finalizeBatchMerkleContext()

	execB.beginBatchMerkleContext()
	execB.WriteKV("a", "1")
	execB.WriteKV("z", "26")
	execB.finalizeBatchMerkleContext()

	if execA.workingState.MerkleRoot != execB.workingState.MerkleRoot {
		t.Fatalf("expected deterministic root after deferred insertion, got %s vs %s", execA.workingState.MerkleRoot, execB.workingState.MerkleRoot)
	}
	if execA.ReadKV("a") != "1" || execA.ReadKV("z") != "26" {
		t.Fatalf("expected finalized new keys in working state, got %v", execA.workingState.KVStore)
	}
}

func TestExecBlockedRequestResumesAfterNestedResponse(t *testing.T) {
	verifierCh := make(chan map[string]any, 8)
	shimCh := make(chan map[string]any, 8)
	var exec *Exec
	exec = NewExec("exec1", []string{"exec1"}, nil, verifierCh, shimCh, 1,
		func(_ *Exec, request map[string]any, _ int64, _ float64) map[string]any {
			if nestedResponses, ok := exec.GetNestedResponses(request["request_id"]); ok && len(nestedResponses) > 0 {
				nested := nestedResponses[0]
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
		nil,
		requiredExecRunConfig(),
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

	pending, ok := exec.pendingExecResults[1]
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
	exec = NewExec("exec1", []string{"exec1"}, nil, verifierCh, shimCh, 1,
		func(_ *Exec, request map[string]any, _ int64, _ float64) map[string]any {
			op, _ := request["op"].(string)
			requestID := request["request_id"]
			switch op {
			case "block":
				if nestedResponses, ok := exec.GetNestedResponses(requestID); ok && len(nestedResponses) > 0 {
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
		nil,
		requiredExecRunConfig(),
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
	exec = NewExec("exec1", []string{"exec1"}, nil, verifierCh, shimCh, 1,
		func(_ *Exec, request map[string]any, _ int64, _ float64) map[string]any {
			op, _ := request["op"].(string)
			requestID := request["request_id"]
			switch op {
			case "block":
				if nestedResponses, ok := exec.GetNestedResponses(requestID); ok && len(nestedResponses) > 0 {
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
		nil,
		requiredExecRunConfig(),
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
	exec = NewExec("exec1", []string{"exec1"}, nil, verifierCh, shimCh, 1,
		func(_ *Exec, request map[string]any, _ int64, _ float64) map[string]any {
			requestID, _ := request["request_id"].(string)
			mu.Lock()
			trace = append(trace, requestID)
			mu.Unlock()
			if nestedResponses, ok := exec.GetNestedResponses(requestID); ok && len(nestedResponses) > 0 {
				return map[string]any{"request_id": requestID, "status": "ok"}
			}
			return map[string]any{"request_id": requestID, "status": "blocked_for_nested_response"}
		},
		nil,
		requiredExecRunConfig(),
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

func TestExecForceSequentialDisablesParallelYield(t *testing.T) {
	verifierCh := make(chan map[string]any, 8)
	shimCh := make(chan map[string]any, 8)
	var exec *Exec
	exec = NewExec("exec1", []string{"exec1"}, nil, verifierCh, shimCh, 1,
		func(_ *Exec, request map[string]any, _ int64, _ float64) map[string]any {
			op, _ := request["op"].(string)
			requestID := request["request_id"]
			switch op {
			case "block":
				if nestedResponses, ok := exec.GetNestedResponses(requestID); ok && len(nestedResponses) > 0 {
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
		nil,
		requiredExecRunConfig(),
	)
	exec.forceSequential = true

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

	time.Sleep(120 * time.Millisecond)
	if got := exec.ReadKV("next_batch_progress"); got == "yes" {
		t.Fatalf("expected sequential mode to avoid yielding to later parallel batches")
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

	if got := exec.ReadKV("next_batch_progress"); got != "yes" {
		t.Fatalf("expected later batch to run after blocked request resumed")
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

	pendingA, ok := execA.pendingExecResults[1]
	if !ok {
		t.Fatalf("expected pending response on execA")
	}
	pendingB, ok := execB.pendingExecResults[1]
	if !ok {
		t.Fatalf("expected pending response on execB")
	}
	if pendingA.token != pendingB.token {
		t.Fatalf("expected deterministic tokens across execs, got %s vs %s", pendingA.token, pendingB.token)
	}
}
