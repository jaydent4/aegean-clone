package aegeanworkflow

import (
	"aegean/common"
	"aegean/components/exec"
)

const fanoutBaseResponseContextKey = "fanout_base_response"

func executeFanoutBase(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64, targetNodes map[string]struct{}, everyN uint64) map[string]any {
	requestID := request["request_id"]
	nestedResponses, hasNested := e.GetNestedResponses(requestID)
	_, started := e.GetRequestContextValue(requestID, fanoutBaseResponseContextKey)

	// Stage 1: no local continuation context and no cached nested response.
	if !started && (!hasNested || len(nestedResponses) == 0) {
		response := executeRequestBase(e, request, ndSeed, ndTimestamp, targetNodes, everyN)
		if !e.SetRequestContextValue(requestID, fanoutBaseResponseContextKey, response) {
			return map[string]any{
				"status":     "error",
				"request_id": requestID,
				"error":      "failed to initialize request continuation context",
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

		return map[string]any{
			"status":     "blocked_for_nested_response",
			"request_id": requestID,
		}
	}

	// Stage 2: either continuation context exists or a nested response is cached.
	if !hasNested || len(nestedResponses) == 0 {
		return map[string]any{
			"status":     "blocked_for_nested_response",
			"request_id": requestID,
		}
	}
	nested := nestedResponses[0]
	if fanoutDone, output := processNestedFanoutResponse(e, requestID, nested); fanoutDone {
		e.ClearRequestContext(requestID)
		return output
	}
	return map[string]any{
		"status":     "blocked_for_nested_response",
		"request_id": requestID,
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
