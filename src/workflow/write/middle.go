package writeworkflow

import (
	"aegean/aegean/exec"
	"aegean/common"
)

const (
	writeMiddleStageContextKey = "write_middle_stage"

	writeMiddleStageAwaitNested = "await_nested"
)

var writeBackendTargets = []string{"node4"}

func ExecuteRequestMiddle(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	stageAny, _ := e.GetRequestContextValue(requestID, writeMiddleStageContextKey)
	stage, _ := stageAny.(string)

	switch stage {
	case "":
		writeConfiguredValue(e, requestID, "middle")
		if !e.SetRequestContextValue(requestID, writeMiddleStageContextKey, writeMiddleStageAwaitNested) {
			return map[string]any{
				"request_id": requestID,
				"status":     "error",
				"error":      "failed to initialize request continuation context",
			}
		}

		nestedRequest := map[string]any{
			"type":       "request",
			"request_id": requestID,
			"timestamp":  request["timestamp"],
		}
		if common.BoolOrDefault(e.RunConfig, "write_nested_use_eo", false) || common.BoolOrDefault(e.RunConfig, "nested_use_eo", false) {
			e.DispatchNestedRequestEO(request, writeBackendTargets, nestedRequest)
		} else {
			e.DispatchNestedRequestDirect(request, writeBackendTargets, nestedRequest)
		}
		return blockedForWriteNestedResponse(requestID)

	case writeMiddleStageAwaitNested:
		nestedResponses, hasNested := e.GetNestedResponses(requestID)
		if !hasNested || len(nestedResponses) == 0 {
			return blockedForWriteNestedResponse(requestID)
		}
		nested := nestedResponses[0]
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
			return blockedForWriteNestedResponse(requestID)
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

func blockedForWriteNestedResponse(requestID any) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"status":     "blocked_for_nested_response",
	}
}
