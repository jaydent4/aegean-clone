package spinworkflow

import (
	"aegean/aegean/exec"
	"aegean/common"
	"fmt"
	"time"
)

const (
	spinMiddleStageContextKey  = "spin_middle_stage"
	spinMiddleStageAwaitNested = "await_nested"
	spinChainStageAwaitNested  = "await_nested"
	spinWideNextBackendKey     = "spin_wide_next_backend"

	defaultSpinMs = 1
)

var spinBackendTargets = []string{"node4"}

func InitState(e *exec.Exec) map[string]string {
	_ = e
	return map[string]string{}
}

func ExecuteRequestChain(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
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

		if common.BoolOrDefault(e.RunConfig, "nested_use_eo", false) {
			e.DispatchNestedRequestEO(request, targets, nestedSpinRequestFrom(request))
		} else {
			e.DispatchNestedRequestDirect(request, targets, nestedSpinRequestFrom(request))
		}
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

func ExecuteRequestWideMiddle(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
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
		if _, ok := e.GetNestedResponseByRequestID(requestID, childID); !ok {
			targets := spinWideBackendTargets(e.RunConfig, nextBackend)
			if len(targets) == 0 {
				return spinErrorResponse(requestID, fmt.Sprintf("missing backend%d service targets", nextBackend))
			}
			if common.BoolOrDefault(e.RunConfig, "nested_use_eo", false) {
				e.DispatchNestedRequestEO(request, targets, nestedSpinWideRequestFrom(request, nextBackend))
			} else {
				e.DispatchNestedRequestDirect(request, targets, nestedSpinWideRequestFrom(request, nextBackend))
			}
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
		return spinOKResponse(requestID)

	default:
		return spinErrorResponse(requestID, "unknown middle stage: "+stage)
	}
}

func ExecuteRequestBackend(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	spinForRequest(e.RunConfig)
	return spinOKResponseFromRequest(request)
}

func spinForRequest(runConfig map[string]any) {
	spinMs := spinMillisecondsOrDefault(runConfig, "spin_ms", float64(common.IntOrDefault(runConfig, "worker_spin_ms", defaultSpinMs)))
	if spinMs <= 0 {
		return
	}

	deadline := time.Now().Add(time.Duration(spinMs * float64(time.Millisecond)))
	for time.Now().Before(deadline) {
	}
}

func spinMillisecondsOrDefault(runConfig map[string]any, key string, defaultValue float64) float64 {
	value, ok := runConfig[key]
	if !ok {
		return defaultValue
	}
	switch typed := value.(type) {
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case float64:
		return typed
	case float32:
		return float64(typed)
	default:
		panic(fmt.Sprintf("run config field %q must be a number", key))
	}
}

func spinChainStage(runConfig map[string]any) int {
	return common.IntOrDefault(runConfig, "spin_stage", 1)
}

func spinChainDepth(runConfig map[string]any) int {
	return common.IntOrDefault(runConfig, "depth", 1)
}

func spinChainIsTerminal(runConfig map[string]any) bool {
	return spinChainStage(runConfig) >= spinChainDepth(runConfig)
}

func spinChainNextTargets(runConfig map[string]any) []string {
	nextService := common.StringOrDefault(runConfig, "spin_next_service", "")
	if nextService == "" {
		return nil
	}
	return common.ServiceNodesOrDefault(runConfig, nextService, nil)
}

func spinChainStageContextKey(runConfig map[string]any) string {
	return fmt.Sprintf("spin_chain_stage_%d", spinChainStage(runConfig))
}

func spinWideWidth(runConfig map[string]any) int {
	return common.IntOrDefault(runConfig, "width", 1)
}

func spinWideNextBackend(runConfig map[string]any, getContext func() (any, bool)) int {
	value, ok := getContext()
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
		panic(fmt.Sprintf("run config field %q must be an integer", spinWideNextBackendKey))
	}
}

func spinWideBackendTargets(runConfig map[string]any, backendIndex int) []string {
	serviceName := fmt.Sprintf("%s%d", common.StringOrDefault(runConfig, "spin_wide_backend_prefix", "backend"), backendIndex)
	return common.ServiceNodesOrDefault(runConfig, serviceName, nil)
}

func spinWideChildRequestID(requestID any, backendIndex int) string {
	return fmt.Sprintf("%v/backend%d", requestID, backendIndex)
}

func spinOKResponse(requestID any) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"status":     "ok",
	}
}

func spinOKResponseFromRequest(request map[string]any) map[string]any {
	response := spinOKResponse(request["request_id"])
	if parentRequestID, ok := request["parent_request_id"]; ok && parentRequestID != nil {
		response["parent_request_id"] = parentRequestID
	}
	return response
}

func spinErrorResponse(requestID any, message string) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"status":     "error",
		"error":      message,
	}
}

func blockedForSpinNestedResponse(requestID any) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"status":     "blocked_for_nested_response",
	}
}

func hasSpinNestedResponseByRequestID(nestedResponses []map[string]any, requestID string) bool {
	for _, nested := range nestedResponses {
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
			continue
		}
		nestedRequestID, _ := nested["request_id"].(string)
		if nestedRequestID == requestID {
			return true
		}
	}
	return false
}

func nestedSpinRequestFrom(request map[string]any) map[string]any {
	nested := make(map[string]any, len(request)+1)
	for key, value := range request {
		nested[key] = value
	}
	nested["type"] = "request"
	return nested
}

func nestedSpinWideRequestFrom(request map[string]any, backendIndex int) map[string]any {
	nested := nestedSpinRequestFrom(request)
	parentRequestID := request["request_id"]
	nested["request_id"] = spinWideChildRequestID(parentRequestID, backendIndex)
	nested["parent_request_id"] = parentRequestID
	nested["spin_wide_backend_index"] = backendIndex
	return nested
}
