package spinworkflow

import (
	"aegean/aegean/unreplicated"
	"aegean/common"
)

const spinDirectMiddleStageContextKey = "spin_direct_middle_stage"

func InitStateDirect(e *unreplicated.Engine) map[string]string {
	_ = e
	return map[string]string{}
}

func ExecuteRequestMiddleDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	stageAny, _ := e.GetRequestContextValue(requestID, spinDirectMiddleStageContextKey)
	stage, _ := stageAny.(string)

	switch stage {
	case "":
		spinForRequest(e.RunConfig)
		if !e.SetRequestContextValue(requestID, spinDirectMiddleStageContextKey, spinMiddleStageAwaitNested) {
			return map[string]any{
				"request_id": requestID,
				"status":     "error",
				"error":      "failed to initialize request continuation context",
			}
		}

		e.DispatchNestedRequestDirect(request, common.ServiceNodesOrDefault(e.RunConfig, "backend", []string{"node2"}), nestedSpinRequestFrom(request))
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

func ExecuteRequestBackendDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	spinForRequest(e.RunConfig)
	return map[string]any{
		"request_id": request["request_id"],
		"status":     "ok",
	}
}
