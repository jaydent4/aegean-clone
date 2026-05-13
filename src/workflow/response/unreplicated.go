package responseworkflow

import (
	"aegean/aegean/unreplicated"
	"aegean/common"
)

const responseDirectMiddleStageContextKey = "response_direct_middle_stage"

func InitStateDirect(e *unreplicated.Engine) map[string]string {
	_ = e
	return map[string]string{}
}

func ExecuteRequestBackendDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	return makeBackendResponse(e.RunConfig, request["request_id"])
}

func ExecuteRequestMiddleDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	stageAny, _ := e.GetRequestContextValue(requestID, responseDirectMiddleStageContextKey)
	stage, _ := stageAny.(string)

	switch stage {
	case "":
		if !e.SetRequestContextValue(requestID, responseDirectMiddleStageContextKey, responseMiddleStageAwaitNested) {
			return map[string]any{
				"request_id": requestID,
				"status":     "error",
				"error":      "failed to initialize request continuation context",
			}
		}

		e.DispatchNestedRequestDirect(request, common.ServiceNodesOrDefault(e.RunConfig, "backend", []string{"node2"}), nestedRequestFrom(request))
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
