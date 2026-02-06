package nodes

import (
	"net"
	"net/http"
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
	exec := NewExec(name, verifiers, peers, name, verifierCh, shimCh)
	return exec, verifierCh, shimCh
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

	pending, ok := exec.pendingResponses[1]
	if !ok {
		t.Fatalf("expected pending response for seq_num 1")
	}
	if len(pending.outputs) != 2 {
		t.Fatalf("expected 2 outputs, got %d", len(pending.outputs))
	}

	expectedToken := exec.computeStateHash(exec.kvStore, pending.outputs, exec.prevHash, 1)
	if pending.token != expectedToken {
		t.Fatalf("expected pending token %s, got %s", expectedToken, pending.token)
	}
	exec.kvStore["mutate"] = "later"
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

	if exec.stableSeqNum != 2 {
		t.Fatalf("expected stableSeqNum 2, got %d", exec.stableSeqNum)
	}
	if exec.prevHash != pending.token {
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
	exec.stableSeqNum = 1
	exec.stableState = map[string]string{"x": "1"}
	exec.kvStore = copyStringMap(exec.stableState)

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

	if exec.stableSeqNum != 5 {
		t.Fatalf("expected stableSeqNum updated to 5, got %d", exec.stableSeqNum)
	}
	if exec.prevHash != "hash-after-transfer" {
		t.Fatalf("expected prevHash updated after transfer")
	}
	if exec.forceSequential {
		t.Fatalf("expected forceSequential false after successful state transfer")
	}
	if exec.kvStore["a"] != "10" || exec.kvStore["b"] != "20" {
		t.Fatalf("expected kvStore to match transferred state, got %v", exec.kvStore)
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
	resp2 := exec.HandleMessage(batch2)
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
	exec.HandleMessage(batch1)

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
	resp := exec.HandleMessage(verifyResp)
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
	exec.HandleMessage(batch1)

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
	exec.stableState = map[string]string{"stable": "yes"}
	exec.kvStore = map[string]string{"dirty": "no"}

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

	if exec.kvStore["stable"] != "yes" || len(exec.kvStore) != 1 {
		t.Fatalf("expected rollback to stable state, got %v", exec.kvStore)
	}
	if !exec.forceSequential {
		t.Fatalf("expected forceSequential true after rollback")
	}
}

// Rollback decision reverts to stable state and forces sequential execution
func TestExecRollbackDecisionForcesSequential(t *testing.T) {
	exec, _, _ := newTestExec("exec1", nil, nil)
	exec.stableState = map[string]string{"stable": "yes"}
	exec.kvStore = map[string]string{"dirty": "no"}
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

	if exec.kvStore["stable"] != "yes" || len(exec.kvStore) != 1 {
		t.Fatalf("expected rollback to stable state, got %v", exec.kvStore)
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
