package aegeanworkflow

import (
	"aegean/aegean/exec"
	"aegean/common"
	"aegean/telemetry"

	"go.opentelemetry.io/otel/attribute"
)

const (
	fanoutStageContextKey        = "fanout_stage"
	fanoutBaseResponseContextKey = "fanout_base_response"
	fanoutWaitSpanContextKey     = "fanout_wait_span"

	fanoutStageAwaitNested = "await_nested"
)

func blockedForNested(requestID any) map[string]any {
	return map[string]any{
		"status":     "blocked_for_nested_response",
		"request_id": requestID,
	}
}

func executeFanoutBase(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64, targetNodes map[string]struct{}, everyN uint64) map[string]any {
	requestID := request["request_id"]
	stageAny, _ := e.GetRequestContextValue(requestID, fanoutStageContextKey)
	stage, _ := stageAny.(string)

	switch stage {
	case "":
		// Stage 1: execute local work, cache base response, fanout nested requests, then block.
		response := executeRequestBase(e, request, ndSeed, ndTimestamp, targetNodes, everyN)
		if !e.SetRequestContextValue(requestID, fanoutBaseResponseContextKey, response) {
			return map[string]any{
				"status":     "error",
				"request_id": requestID,
				"error":      "failed to initialize request continuation context",
			}
		}
		if !e.SetRequestContextValue(requestID, fanoutStageContextKey, fanoutStageAwaitNested) {
			return map[string]any{
				"status":     "error",
				"request_id": requestID,
				"error":      "failed to set fanout stage context",
			}
		}
		_, waitSpan := telemetry.StartSpanFromPayload(
			request,
			"workflow.middle.nested_wait",
			append(
				telemetry.AttrsFromPayload(request),
				attribute.String("node.name", e.Name),
			)...,
		)
		_ = e.SetRequestContextValue(requestID, fanoutWaitSpanContextKey, waitSpan)

		nestedRequest := map[string]any{
			"type":       "request",
			"request_id": requestID,
			"timestamp":  request["timestamp"],
			"op":         request["op"],
			"op_payload": request["op_payload"],
		}
		if common.BoolOrDefault(e.RunConfig, "aegean_nested_use_eo", false) {
			e.DispatchNestedRequestEO(request, []string{"node4"}, nestedRequest)
		} else {
			e.DispatchNestedRequestDirect(request, []string{"node4"}, nestedRequest)
		}
		return blockedForNested(requestID)

	case fanoutStageAwaitNested:
		// Stage 2: wait for nested result, then finalize and clean up.
		nestedResponses, hasNested := e.GetNestedResponses(requestID)
		if !hasNested || len(nestedResponses) == 0 {
			return blockedForNested(requestID)
		}
		nested := nestedResponses[0]
		if fanoutDone, output := processNestedFanoutResponse(e, requestID, nested); fanoutDone {
			e.StartRequestWaitSpan(
				request,
				exec.PostNestedVerifyGateWaitSpanKey(),
				"workflow.middle.post_nested_verify_gate_wait",
				attribute.Int("batch.seq_num", requestBatchSeq(e, requestID)),
				attribute.Int("gate.next_verify_seq", nextVerifySeq(e)),
				attribute.Int("gate.stable_seq_num", stableSeq(e)),
			)
			if waitSpanAny, ok := e.GetRequestContextValue(requestID, fanoutWaitSpanContextKey); ok {
				if waitSpan, ok := waitSpanAny.(interface{ End() }); ok && waitSpan != nil {
					waitSpan.End()
				}
			}
			e.DeleteRequestContextValue(requestID, fanoutStageContextKey)
			e.DeleteRequestContextValue(requestID, fanoutBaseResponseContextKey)
			e.DeleteRequestContextValue(requestID, fanoutWaitSpanContextKey)
			return output
		}
		return blockedForNested(requestID)

	default:
		return map[string]any{
			"status":     "error",
			"request_id": requestID,
			"error":      "unknown fanout stage: " + stage,
		}
	}
}

func ExecuteRequestMiddle(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return executeFanoutBase(e, request, ndSeed, ndTimestamp, map[string]struct{}{}, 0)
}

func ExecuteRequestMiddleDivergeOneNode(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return executeFanoutBase(e, request, ndSeed, ndTimestamp, map[string]struct{}{
		"node1": {},
	}, 4)
}

func ExecuteRequestMiddleDivergeTwoNode(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return executeFanoutBase(e, request, ndSeed, ndTimestamp, map[string]struct{}{
		"node1": {},
		"node2": {},
	}, 20)
}

func ExecuteRequestMiddleDivergeThreeNode(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return executeFanoutBase(e, request, ndSeed, ndTimestamp, map[string]struct{}{
		"node1": {},
		"node2": {},
		"node3": {},
	}, 20)
}

func processNestedFanoutResponse(e *exec.Exec, requestID any, nested map[string]any) (bool, map[string]any) {
	if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
		return false, nil
	}

	output := map[string]any{
		"request_id": requestID,
		"status":     "ok",
	}
	if baseAny, ok := e.GetRequestContextValue(requestID, fanoutBaseResponseContextKey); ok && baseAny != nil {
		if base, ok := baseAny.(map[string]any); ok {
			for key, value := range base {
				if _, exists := output[key]; !exists {
					output[key] = value
				}
			}
		}
	}
	// Keep the client-facing response flat so it matches the expected response shape.
	if selectedResponse, ok := nested["response"].(map[string]any); ok {
		for key, value := range selectedResponse {
			if _, exists := output[key]; !exists {
				output[key] = value
			}
		}
	}
	return true, output
}

func requestBatchSeq(e *exec.Exec, requestID any) int {
	value, ok := e.GetRequestContextValue(requestID, "request_batch_seq")
	if !ok {
		return 0
	}
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

func nextVerifySeq(e *exec.Exec) int {
	next, _ := e.RequestVerifyGateSnapshot()
	return next
}

func stableSeq(e *exec.Exec) int {
	_, stable := e.RequestVerifyGateSnapshot()
	return stable
}
