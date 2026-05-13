package responseworkflow

import (
	"aegean/aegean/exec"
	"aegean/common"
)

var responseBackendTargets = []string{"node4"}

func InitState(e *exec.Exec) map[string]string {
	_ = e
	return map[string]string{}
}

func ExecuteRequestBackend(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = e
	_ = ndSeed
	_ = ndTimestamp

	return makeBackendResponse(e.RunConfig, request["request_id"])
}

func ExecuteRequestMiddle(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	stageAny, _ := e.GetRequestContextValue(requestID, responseMiddleStageContextKey)
	stage, _ := stageAny.(string)

	switch stage {
	case "":
		if !e.SetRequestContextValue(requestID, responseMiddleStageContextKey, responseMiddleStageAwaitNested) {
			return map[string]any{
				"request_id": requestID,
				"status":     "error",
				"error":      "failed to initialize request continuation context",
			}
		}

		if common.BoolOrDefault(e.RunConfig, "response_nested_use_eo", false) || common.BoolOrDefault(e.RunConfig, "nested_use_eo", false) {
			e.DispatchNestedRequestEO(request, responseBackendTargets, nestedRequestFrom(request))
		} else {
			e.DispatchNestedRequestDirect(request, responseBackendTargets, nestedRequestFrom(request))
		}
		return blockedForNestedResponse(requestID)

	case responseMiddleStageAwaitNested:
		nestedResponses, hasNested := e.GetNestedResponses(requestID)
		if !hasNested || len(nestedResponses) == 0 {
			return blockedForNestedResponse(requestID)
		}
		nested := nestedResponses[0]
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
			return blockedForNestedResponse(requestID)
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
