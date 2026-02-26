package aegeanworkflow

import (
	"aegean/common"
	"aegean/components/exec"
)

const (
	fanoutStageContextKey        = "fanout_stage"
	fanoutBaseResponseContextKey = "fanout_base_response"

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

		fanoutTargets := []string{"node5", "node6", "node7"}
		for _, target := range fanoutTargets {
			outgoing := map[string]any{
				"type":       "request",
				"request_id": requestID,
				"timestamp":  request["timestamp"],
				"sender":     e.Name,
				"op":         request["op"],
				"op_payload": request["op_payload"],
			}
			go func(target string, outgoing map[string]any) {
				_, err := common.SendMessage(target, 8000, outgoing)
				if err != nil {
				}
			}(target, outgoing)
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
			e.ClearRequestContext(requestID)
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
		"node2": {},
	}, 4)
}

func ExecuteRequestMiddleDivergeTwoNode(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return executeFanoutBase(e, request, ndSeed, ndTimestamp, map[string]struct{}{
		"node2": {},
		"node3": {},
	}, 5)
}

func ExecuteRequestMiddleDivergeThreeNode(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return executeFanoutBase(e, request, ndSeed, ndTimestamp, map[string]struct{}{
		"node2": {},
		"node3": {},
		"node4": {},
	}, 5)
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
	// Keep client-facing response flat so it matches expected_result shape in traces
	if selectedResponse, ok := nested["response"].(map[string]any); ok {
		for key, value := range selectedResponse {
			if _, exists := output[key]; !exists {
				output[key] = value
			}
		}
	}
	return true, output
}
