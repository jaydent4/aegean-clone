package reqraceworkflow

import (
	"aegean/common"
	"aegean/components/exec"
)

const (
	middleStageContextKey = "req_race_middle_stage"

	middleStageAwaitNested = "await_nested"
)

var backend1Targets = []string{"node4", "node5", "node6"}
var backend2Targets = []string{"node7", "node8", "node9"}
var backend3Targets = []string{"node10", "node11", "node12"}

// ExecuteRequestMiddle fans out to both backends and completes on the first nested response.
func ExecuteRequestMiddle(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	stageAny, _ := e.GetRequestContextValue(requestID, middleStageContextKey)
	stage, _ := stageAny.(string)

	switch stage {
	case "":
		// Stage 1: send nested fanout and transition to await stage
		if !e.SetRequestContextValue(requestID, middleStageContextKey, middleStageAwaitNested) {
			return map[string]any{
				"request_id": requestID,
				"status":     "error",
				"error":      "failed to initialize request continuation context",
			}
		}

		dispatchNestedFanout(e, request)
		return blockedForNestedResponse(requestID)

	case middleStageAwaitNested:
		// Stage 2: wait for nested response, finalize when shim quorum aggregation is present
		nestedResponses, hasNested := e.GetNestedResponses(requestID)
		if !hasNested || len(nestedResponses) == 0 {
			return blockedForNestedResponse(requestID)
		}
		nested := nestedResponses[0]
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
			return blockedForNestedResponse(requestID)
		}

		response := map[string]any{
			"request_id": requestID,
			"status":     "ok",
		}
		if nestedSender, _ := nested["sender"].(string); nestedSender != "" {
			response["backend_sender"] = nestedSender
		}
		if nestedResponse, ok := nested["response"].(map[string]any); ok {
			for key, value := range nestedResponse {
				if _, exists := response[key]; !exists {
					response[key] = value
				}
			}
		}

		e.ClearRequestContext(requestID)
		return response

	default:
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "unknown middle stage: " + stage,
		}
	}
}

func blockedForNestedResponse(requestID any) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"status":     "blocked_for_nested_response",
	}
}

func dispatchNestedFanout(e *exec.Exec, request map[string]any) {
	requestID := request["request_id"]
	sendFanoutRequest := func(target string, outgoing map[string]any) {
		_, err := common.SendMessage(target, 8000, outgoing)
		if err != nil {
		}
	}

	groups := [][]string{backend1Targets, backend2Targets, backend3Targets}
	for _, targets := range groups {
		for i := 0; i < 2; i++ {
			for _, target := range targets {
				outgoing := map[string]any{
					"type":       "request",
					"request_id": requestID,
					"timestamp":  request["timestamp"],
					"sender":     e.Name,
					"op":         "default",
					"op_payload": map[string]any{},
				}
				go sendFanoutRequest(target, outgoing)
			}
		}
	}
}
