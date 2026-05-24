package spinworkflow

import (
	"aegean/aegean/exec"
	"aegean/common"
	"time"
)

const (
	spinMiddleStageContextKey  = "spin_middle_stage"
	spinMiddleStageAwaitNested = "await_nested"

	defaultSpinMs = 1
)

var spinBackendTargets = []string{"node4"}

func InitState(e *exec.Exec) map[string]string {
	_ = e
	return map[string]string{}
}

func ExecuteRequestMiddle(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	stageAny, _ := e.GetRequestContextValue(requestID, spinMiddleStageContextKey)
	stage, _ := stageAny.(string)

	switch stage {
	case "":
		spinForRequest(e.RunConfig)
		if !e.SetRequestContextValue(requestID, spinMiddleStageContextKey, spinMiddleStageAwaitNested) {
			return map[string]any{
				"request_id": requestID,
				"status":     "error",
				"error":      "failed to initialize request continuation context",
			}
		}

		if common.BoolOrDefault(e.RunConfig, "worker_nested_use_eo", false) || common.BoolOrDefault(e.RunConfig, "nested_use_eo", false) {
			e.DispatchNestedRequestEO(request, spinBackendTargets, nestedSpinRequestFrom(request))
		} else {
			e.DispatchNestedRequestDirect(request, spinBackendTargets, nestedSpinRequestFrom(request))
		}
		return blockedForSpinNestedResponse(requestID)

	case spinMiddleStageAwaitNested:
		nestedResponses, hasNested := e.GetNestedResponses(requestID)
		if !hasNested || len(nestedResponses) == 0 {
			return blockedForSpinNestedResponse(requestID)
		}
		nested := nestedResponses[0]
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
			return blockedForSpinNestedResponse(requestID)
		}

		e.ClearRequestContext(requestID)
		return map[string]any{
			"request_id": requestID,
			"status":     "ok",
		}

	default:
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "unknown middle stage: " + stage,
		}
	}
}

func ExecuteRequestBackend(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	spinForRequest(e.RunConfig)
	return map[string]any{
		"request_id": request["request_id"],
		"status":     "ok",
	}
}

func spinForRequest(runConfig map[string]any) {
	spinMs := common.IntOrDefault(runConfig, "worker_spin_ms", defaultSpinMs)
	if spinMs <= 0 {
		return
	}

	deadline := time.Now().Add(time.Duration(spinMs) * time.Millisecond)
	for time.Now().Before(deadline) {
	}
}

func blockedForSpinNestedResponse(requestID any) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"status":     "blocked_for_nested_response",
	}
}

func nestedSpinRequestFrom(request map[string]any) map[string]any {
	nested := make(map[string]any, len(request)+1)
	for key, value := range request {
		nested[key] = value
	}
	nested["type"] = "request"
	return nested
}
