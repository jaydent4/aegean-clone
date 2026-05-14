package exec

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"aegean/aegean_opt/protocol"
)

func TestExecStreamingWindowExecutesPlanAndBuffersVerifyResult(t *testing.T) {
	verifierCh := make(chan map[string]any, 8)
	shimCh := make(chan map[string]any, 8)
	exec := NewExec("exec1", []string{"exec1"}, nil, verifierCh, shimCh, 1,
		func(e *Exec, request map[string]any, _ int64, _ float64) map[string]any {
			requestID, _ := request["request_id"].(string)
			if requestID == "r3" {
				if e.ReadKV("seen_r1") != "yes" || e.ReadKV("seen_r2") != "yes" {
					return map[string]any{"request_id": requestID, "status": "dependency_violation"}
				}
			}
			e.WriteKV("seen_"+requestID, "yes")
			return map[string]any{"request_id": requestID, "status": "ok"}
		},
		nil,
		map[string]any{
			"worker_count":      2,
			"parallel_window_k": 2,
		},
	)

	exec.HandleScheduledRequestMessage(streamPlan(1, 0, "r1", 0, nil))
	exec.HandleScheduledRequestMessage(streamPlan(1, 1, "r2", 1, nil))
	exec.HandleScheduledRequestMessage(streamPlan(1, 2, "r3", 1, []string{"1:0", "1:1"}))
	exec.HandleVerificationWindowClosedMessage(map[string]any{
		protocol.FieldType:   protocol.MessageTypeVerificationWindowClosed,
		protocol.FieldSeqNum: 1,
		protocol.FieldCount:  3,
	})

	var pending pendingExecResult
	waitForCondition(t, 2*time.Second, func() bool {
		exec.mu.Lock()
		defer exec.mu.Unlock()
		var ok bool
		pending, ok = exec.pendingExecResults[1]
		return ok && len(pending.outputs) == 3
	}, "expected streaming window to produce a pending verify result")

	for i, want := range []string{"r1", "r2", "r3"} {
		if got := pending.outputs[i]["request_id"]; got != want {
			t.Fatalf("expected output %d for %s, got %v", i, want, got)
		}
		if got := pending.outputs[i]["status"]; got != "ok" {
			t.Fatalf("expected output %d status ok, got %v", i, got)
		}
	}
}

func TestExecStreamingBlockedRequestResumesByRequestID(t *testing.T) {
	verifierCh := make(chan map[string]any, 8)
	shimCh := make(chan map[string]any, 8)
	var calls atomic.Int32
	exec := NewExec("exec1", []string{"exec1"}, nil, verifierCh, shimCh, 1,
		func(e *Exec, request map[string]any, _ int64, _ float64) map[string]any {
			calls.Add(1)
			requestID := request["request_id"]
			if nestedResponses, ok := e.GetNestedResponses(requestID); ok && len(nestedResponses) > 0 {
				return map[string]any{
					"request_id": requestID,
					"status":     "ok",
					"nested":     nestedResponses[0]["status"],
				}
			}
			return map[string]any{
				"request_id": requestID,
				"status":     "blocked_for_nested_response",
			}
		},
		nil,
		map[string]any{
			"worker_count":      1,
			"parallel_window_k": 2,
		},
	)

	exec.HandleScheduledRequestMessage(streamPlan(1, 0, "r1", 0, nil))
	exec.HandleVerificationWindowClosedMessage(map[string]any{
		protocol.FieldType:   protocol.MessageTypeVerificationWindowClosed,
		protocol.FieldSeqNum: 1,
		protocol.FieldCount:  1,
	})

	waitForCondition(t, 2*time.Second, func() bool {
		return calls.Load() >= 1
	}, "expected streaming request to block once")
	if !exec.BufferNestedResponse(map[string]any{"request_id": "r1", "status": "ready"}) {
		t.Fatalf("expected nested response to be accepted")
	}

	var pending pendingExecResult
	waitForCondition(t, 2*time.Second, func() bool {
		exec.mu.Lock()
		defer exec.mu.Unlock()
		var ok bool
		pending, ok = exec.pendingExecResults[1]
		return ok && len(pending.outputs) == 1
	}, "expected streaming window to resume after nested response")

	if calls.Load() < 2 {
		t.Fatalf("expected blocked request to execute again after nested response")
	}
	if got := pending.outputs[0]["status"]; got != "ok" {
		t.Fatalf("expected resumed output status ok, got %v", got)
	}
	if got := pending.outputs[0]["nested"]; got != "ready" {
		t.Fatalf("expected nested status ready, got %v", got)
	}
}

func TestExecStreamingReadyNestedResponseBypassesBlockedWindowHead(t *testing.T) {
	verifierCh := make(chan map[string]any, 8)
	shimCh := make(chan map[string]any, 8)
	var countsMu sync.Mutex
	counts := make(map[string]int)
	exec := NewExec("exec1", []string{"exec1"}, nil, verifierCh, shimCh, 1,
		func(e *Exec, request map[string]any, _ int64, _ float64) map[string]any {
			requestID, _ := request["request_id"].(string)
			countsMu.Lock()
			counts[requestID]++
			countsMu.Unlock()
			if nestedResponses, ok := e.GetNestedResponses(requestID); ok && len(nestedResponses) > 0 {
				return map[string]any{
					"request_id": requestID,
					"status":     "ok",
					"nested":     nestedResponses[0]["status"],
				}
			}
			return map[string]any{
				"request_id": requestID,
				"status":     "blocked_for_nested_response",
			}
		},
		nil,
		map[string]any{
			"worker_count":      1,
			"parallel_window_k": 2,
		},
	)

	exec.HandleScheduledRequestMessage(streamPlan(1, 0, "r1", 0, nil))
	exec.HandleScheduledRequestMessage(streamPlan(1, 1, "r2", 0, nil))
	exec.HandleVerificationWindowClosedMessage(map[string]any{
		protocol.FieldType:   protocol.MessageTypeVerificationWindowClosed,
		protocol.FieldSeqNum: 1,
		protocol.FieldCount:  2,
	})

	waitForCondition(t, 2*time.Second, func() bool {
		countsMu.Lock()
		defer countsMu.Unlock()
		return counts["r1"] >= 1 && counts["r2"] >= 1
	}, "expected both streaming requests to block once")
	exec.scheduler.parallelWindowK = 1
	if !exec.BufferNestedResponse(map[string]any{"request_id": "r2", "status": "ready"}) {
		t.Fatalf("expected nested response for r2 to be accepted")
	}

	waitForCondition(t, 2*time.Second, func() bool {
		countsMu.Lock()
		defer countsMu.Unlock()
		return counts["r2"] >= 2
	}, "expected r2 to resume even though r1 is still waiting at the worker queue head")
}

func streamPlan(seq int, index int, requestID string, worker int, deps []string) map[string]any {
	request := map[string]any{
		"request_id": requestID,
		"op":         "mark",
	}
	return map[string]any{
		protocol.FieldType:        protocol.MessageTypeScheduledRequest,
		protocol.FieldSeqNum:      seq,
		protocol.FieldRequestIdx:  index,
		protocol.FieldRequestID:   requestID,
		protocol.FieldRequest:     request,
		protocol.FieldNodeID:      protocol.NodeID(seq, index),
		protocol.FieldWorker:      worker,
		protocol.FieldDependsOn:   deps,
		protocol.FieldNDSeed:      int64(index + 1),
		protocol.FieldNDTimestamp: float64(index),
	}
}
