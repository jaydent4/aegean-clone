package spinworkflow

import (
	"aegean/aegean/unreplicated"
	"aegean/common"
	"fmt"
)

const spinDirectMiddleStageContextKey = "spin_direct_middle_stage"

func InitStateDirect(e *unreplicated.Engine) map[string]string {
	_ = e
	return map[string]string{}
}

func ExecuteRequestChainDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	stageAny, _ := e.GetRequestContextValue(requestID, spinChainStageContextKey(e.RunConfig))
	stage, _ := stageAny.(string)

	switch stage {
	case "":
		spinForRequest(e.RunConfig)
		if spinChainIsTerminal(e.RunConfig) {
			return spinOKResponse(requestID)
		}
		targets := spinChainNextTargets(e.RunConfig)
		if len(targets) == 0 {
			return spinErrorResponse(requestID, "missing next service targets")
		}
		if !e.SetRequestContextValue(requestID, spinChainStageContextKey(e.RunConfig), spinChainStageAwaitNested) {
			return spinErrorResponse(requestID, "failed to initialize request continuation context")
		}

		e.DispatchNestedRequestDirect(request, targets, nestedSpinRequestFrom(request))
		return blockedForSpinNestedResponse(requestID)

	case spinChainStageAwaitNested:
		nestedResponses, hasNested := e.GetNestedResponses(requestID)
		if !hasNested || len(nestedResponses) == 0 {
			return blockedForSpinNestedResponse(requestID)
		}
		nested := nestedResponses[0]
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
			return blockedForSpinNestedResponse(requestID)
		}

		e.ClearRequestContext(requestID)
		return spinOKResponse(requestID)

	default:
		return spinErrorResponse(requestID, "unknown chain stage: "+stage)
	}
}

func ExecuteRequestWideMiddleDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	nextBackend := spinWideNextBackend(e.RunConfig, func() (any, bool) {
		return e.GetRequestContextValue(requestID, spinWideNextBackendKey)
	})
	if nextBackend == 0 {
		spinForRequest(e.RunConfig)
		nextBackend = 1
		if !e.SetRequestContextValue(requestID, spinWideNextBackendKey, nextBackend) {
			return spinErrorResponse(requestID, "failed to initialize request continuation context")
		}
	}

	for nextBackend <= spinWideWidth(e.RunConfig) {
		childID := spinWideChildRequestID(requestID, nextBackend)
		nestedResponses, _ := e.GetNestedResponses(requestID)
		if !hasSpinNestedResponseByRequestID(nestedResponses, childID) {
			targets := spinWideBackendTargets(e.RunConfig, nextBackend)
			if len(targets) == 0 {
				return spinErrorResponse(requestID, fmt.Sprintf("missing backend%d service targets", nextBackend))
			}
			e.DispatchNestedRequestDirect(request, targets, nestedSpinWideRequestFrom(request, nextBackend))
			return blockedForSpinNestedResponse(requestID)
		}

		nextBackend++
		if nextBackend <= spinWideWidth(e.RunConfig) {
			if !e.SetRequestContextValue(requestID, spinWideNextBackendKey, nextBackend) {
				return spinErrorResponse(requestID, "failed to update request continuation context")
			}
		}
	}

	e.ClearRequestContext(requestID)
	return spinOKResponse(requestID)
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
		return spinOKResponse(requestID)

	default:
		return spinErrorResponse(requestID, "unknown middle stage: "+stage)
	}
}

func ExecuteRequestBackendDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	spinForRequest(e.RunConfig)
	return spinOKResponseFromRequest(request)
}
